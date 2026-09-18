# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Consolidation Extract Context Provider - 定时/按需整理已有记忆。

Given a single memory-type directory, prefetch its existing memory files and let
the ExtractLoop dedup, merge, split, and reorganize them in place while staying
within that type's schema. There are no session messages and no external `--from`
sources: input and output are the same isolation space, so no cross-space routing
is involved.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openviking.server.identity import RequestContext
from openviking.session.memory.dataclass import MemoryFile, MemoryTypeSchema
from openviking.session.memory.memory_isolation_handler import MemoryIsolationHandler
from openviking.session.memory.memory_type_registry import MemoryTypeRegistry
from openviking.session.memory.merge_policy import MEMORY_MERGE_POLICY
from openviking.session.memory.session_extract_context_provider import (
    SessionExtractContextProvider,
)
from openviking.session.memory.tools import add_tool_call_pair_to_messages, get_tool
from openviking.session.memory.utils.language import resolve_output_language_from_text
from openviking.telemetry import tracer
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

_RESERVED_NAMES = {".overview.md", ".abstract.md"}


class ConsolidationExtractContextProvider(SessionExtractContextProvider):
    """Reorganize existing memories of a single type within one isolation space.

    The provider prefetches every non-reserved ``.md`` file discovered under the
    schema directories that the isolation handler exposes (self and/or peer
    spaces), then instructs the model to consolidate them in place. It reuses
    ``SessionExtractContextProvider``'s read/search tooling and page-id tracking,
    but carries no conversation messages.
    """

    include_tool_parts_in_conversation = False
    split_long_text_messages_for_extraction = False

    def __init__(
        self,
        *,
        memory_type: str,
        target_directory: Optional[str] = None,
        instruction: Optional[str] = None,
        memory_registry: Optional[MemoryTypeRegistry] = None,
        output_language: Optional[str] = None,
    ):
        super().__init__(messages=[])
        self.memory_type = memory_type
        # Canonical `--to` directory. When provided, prefetch lists exactly this
        # directory instead of re-deriving it from ctx.user_id, which can be
        # empty/wrong and collapse the URI (viking://user//memories/...).
        self.target_directory = target_directory.rstrip("/") if target_directory else None
        self._registry = memory_registry
        self._instruction_text = (instruction or "").strip()
        # Language is resolved lazily from prefetched content when not supplied.
        self._output_language = output_language or "en"
        self._language_resolved = output_language is not None
        self.prefetched_uris: List[str] = []

    # ── Schema scope: a single memory type inferred from --to ──

    def get_memory_schemas(self, ctx: RequestContext) -> List[MemoryTypeSchema]:
        del ctx
        schema = self._get_registry().get(self.memory_type)
        if schema is None or not schema.enabled:
            raise ValueError(f"Memory schema not found or disabled: {self.memory_type}")
        return [schema]

    def get_tools(self) -> List[str]:
        # Compile is an offline, user-initiated task, so favor an agentic loop:
        # give the model ls/search/read and let it explore the target directory
        # (including subdirectories) itself instead of relying on a one-shot,
        # possibly-incomplete prefetch. ExtractLoop's write-before-read guard
        # still forces a read of any file it intends to modify.
        return ["ls", "search", "read"]

    # ── Instruction ──

    def instruction(self) -> str:
        output_language = self._output_language
        extra = ""
        if self._instruction_text:
            extra = f"\n\n## User Instruction\n{self._instruction_text}\n"
        target = self.target_directory or f"the `{self.memory_type}` memory directory"
        return f"""You are a memory consolidation agent. Reorganize the existing \
`{self.memory_type}` memories under {target} in place so the final collection is \
clean, non-redundant, and conforms to the `{self.memory_type}` schema.

## Workflow
1. Call `ls` with `recursive=true` on the target directory ONCE to see every file
   (including files inside subdirectories) as `relative/path size`.
2. `read` the full content of every file you may merge, split, edit, or delete.
   You MUST read a file before changing it. Do not repeatedly `ls` the same path.
3. Output ONLY the memory operations (no extra text) once you have gathered enough
   context.

## What to do
- Merge only memories that share the same identity under the schema.
- Split files that mix multiple identities.
- Deduplicate repeated or paraphrased facts, preserving every distinct atomic fact.
- Normalize fields to the schema; do not invent new facts.

## Critical
- Only ls/search/read are available for exploration - there is no write tool; all
  changes are expressed as the final operations.
- Do NOT create memories from nothing; only reorganize what already exists.
- The system generates URIs from memory_type and fields; just provide correct fields.

## Target Output Language
All memory content MUST be written in {output_language}.
{extra}
{MEMORY_MERGE_POLICY}
"""

    # ── Prefetch: seed the loop with a recursive file listing, not content ──

    async def prefetch(self) -> List[Dict[str, Any]]:
        schema = self._get_registry().get(self.memory_type)
        if schema is None or not schema.directory:
            return []

        directories = self._render_directories(schema)
        prefetch_messages: List[Dict[str, Any]] = []
        call_id = 0
        # Seed with a recursive ls of each target directory so the model sees every
        # file (including those in subdirectories) up front and can go straight to
        # read, instead of re-listing directories. Content is not fetched here.
        for directory in directories:
            listing = await self._recursive_listing(directory)
            add_tool_call_pair_to_messages(
                messages=prefetch_messages,
                call_id=call_id,
                tool_name="ls",
                params={"uri": directory, "recursive": True},
                result=listing,
            )
            call_id += 1

        prefetch_messages.append(
            {
                "role": "user",
                "content": (
                    f"Above is a recursive listing of the `{self.memory_type}` memory "
                    "directory (relative paths, no content). read the full content of "
                    "every file you intend to merge, split, edit, or delete (you MUST "
                    "read a file before changing it); use search only if you need more "
                    "context. Then output ALL consolidation operations in a single "
                    "response. If nothing needs reorganizing, return an empty operation set."
                ),
            }
        )
        return prefetch_messages

    async def _recursive_listing(self, directory: str) -> Any:
        """Return a recursive 'relative/path size' listing via the shared ls tool."""
        tool = get_tool("ls")
        if tool is None:
            from openviking.session.memory.tools import MemoryLsTool

            tool = MemoryLsTool()
        return await tool.execute(self.create_tool_context(), uri=directory, recursive=True)

    async def _raw_ls(self, directory: str) -> List[Dict[str, Any]]:
        if not self._viking_fs:
            return []
        try:
            return await self._viking_fs.ls(directory, output="original", ctx=self._ctx) or []
        except Exception as exc:
            if not self._is_expected_read_not_found(exc):
                tracer.info(f"Consolidation: failed to list {directory}: {exc}")
            return []

    def _render_directories(self, schema: MemoryTypeSchema) -> List[str]:
        # Prefer the explicit canonical --to directory so listing does not depend
        # on ctx.user_id (which may be empty and collapse the URI).
        if self.target_directory:
            return [self.target_directory]
        if self._isolation_handler is not None:
            return list(dict.fromkeys(self._isolation_handler.render_schema_directories(schema)))
        return []

    async def _list_memory_files(self, directory: str) -> List[str]:
        """List non-reserved ``.md`` files directly under ``directory``.

        Retained as a helper for callers that want a flat file inventory; the
        agentic prefetch itself only seeds a shallow ls and lets the model
        explore. Not used on the primary path.
        """
        entries = await self._raw_ls(directory)
        uris: List[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("isDir"):
                continue
            name = str(entry.get("name", ""))
            uri = str(entry.get("uri", ""))
            if not uri.endswith(".md"):
                continue
            if (
                name in _RESERVED_NAMES
                or uri.endswith("/.overview.md")
                or uri.endswith("/.abstract.md")
            ):
                continue
            uris.append(uri)
        return uris

    def _resolve_output_language(self) -> None:
        if self._language_resolved:
            return
        texts: List[str] = []
        for mf in self._read_file_contents.values():
            if isinstance(mf, MemoryFile) and mf.content:
                texts.append(mf.content)
        if texts:
            self._output_language = resolve_output_language_from_text(
                "\n".join(texts), fallback_language="en"
            )
        self._language_resolved = True

    def get_output_language(self) -> str:
        return self._output_language

    def _get_registry(self) -> MemoryTypeRegistry:
        if self._registry is None:
            self._registry = MemoryTypeRegistry(load_schemas=True)
        return self._registry


def build_consolidation_isolation_handler(
    ctx: RequestContext,
    extract_context: Any,
    *,
    memory_type: str,
    peer_id: Optional[str],
) -> MemoryIsolationHandler:
    """Build the isolation handler for one consolidation space.

    ``peer_id=None`` consolidates the caller's self memory; a set ``peer_id``
    consolidates only that peer's space. There is no cross-space mixing.
    """
    if peer_id:
        return MemoryIsolationHandler(
            ctx,
            extract_context,
            allowed_memory_types={memory_type},
            allow_self=False,
            allowed_peer_ids={peer_id},
        )
    return MemoryIsolationHandler(
        ctx,
        extract_context,
        allowed_memory_types={memory_type},
        allow_self=True,
    )


__all__ = [
    "ConsolidationExtractContextProvider",
    "build_consolidation_isolation_handler",
]
