# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Tests for `ov compile --skill memory` in-place memory consolidation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.service.compile_service import MEMORY_COMPILE_SKILL, CompileRequest
from openviking.service.memory_compile import (
    _memory_type_from_target,
    _peer_id_from_memory_uri,
)
from openviking.session.memory.consolidation_context_provider import (
    ConsolidationExtractContextProvider,
    build_consolidation_isolation_handler,
)
from openviking.session.memory.memory_updater import ExtractContext
from openviking_cli.exceptions import InvalidArgumentError
from openviking_cli.session.user_id import UserIdentifier


def _ctx(user_id: str = "u1") -> RequestContext:
    return RequestContext(
        user=UserIdentifier(account_id="acc", user_id=user_id),
        role=Role.USER,
    )


# ── CompileRequest memory-mode validation ──


def test_compile_request_memory_mode_needs_no_from():
    request = CompileRequest(**{"to": "viking://user/u1/memories/entities", "skill": "memory"})
    assert request.is_memory_mode is True
    assert request.skill == MEMORY_COMPILE_SKILL
    assert request.from_ == []


def test_compile_request_memory_mode_rejects_from():
    with pytest.raises(ValueError):
        CompileRequest(
            **{
                "to": "viking://user/u1/memories/entities",
                "skill": "memory",
                "from": ["viking://resources/x"],
            }
        )


def test_compile_request_normal_mode_requires_from():
    with pytest.raises(ValueError):
        CompileRequest(**{"to": "viking://resources/wiki", "skill": "viking://agent/skills/wiki"})

    request = CompileRequest(
        **{
            "to": "viking://resources/wiki",
            "skill": "viking://agent/skills/wiki",
            "from": ["viking://resources/y"],
        }
    )
    assert request.is_memory_mode is False
    assert request.from_ == ["viking://resources/y"]


# ── Target parsing helpers ──


def test_memory_type_from_target_reads_type_segment():
    assert _memory_type_from_target("viking://user/u1/memories/entities") == "entities"
    assert (
        _memory_type_from_target("viking://user/u1/peers/agent_x/memories/experiences")
        == "experiences"
    )


def test_memory_type_from_target_rejects_non_memory_or_root():
    with pytest.raises(InvalidArgumentError):
        _memory_type_from_target("viking://resources/wiki")
    with pytest.raises(InvalidArgumentError):
        _memory_type_from_target("viking://user/u1/memories")


def test_peer_id_from_memory_uri():
    assert _peer_id_from_memory_uri("viking://user/u1/memories/entities") is None
    assert _peer_id_from_memory_uri("viking://user/u1/peers/agent_x/memories/entities") == "agent_x"


# ── Isolation handler scope ──


def test_consolidation_isolation_self_vs_peer():
    ctx = _ctx()
    self_handler = build_consolidation_isolation_handler(
        ctx, ExtractContext([]), memory_type="entities", peer_id=None
    )
    assert self_handler.allow_self is True
    assert self_handler.allowed_peer_ids == set()

    peer_handler = build_consolidation_isolation_handler(
        ctx, ExtractContext([]), memory_type="entities", peer_id="agent_x"
    )
    assert peer_handler.allow_self is False
    assert peer_handler.allowed_peer_ids == {"agent_x"}


# ── Provider schema scope and prefetch ──


def test_provider_loads_single_schema_from_to():
    ctx = _ctx()
    provider = ConsolidationExtractContextProvider(memory_type="entities")
    schemas = provider.get_memory_schemas(ctx)
    assert [s.memory_type for s in schemas] == ["entities"]


def test_provider_unknown_type_raises():
    provider = ConsolidationExtractContextProvider(memory_type="does_not_exist")
    with pytest.raises(ValueError):
        provider.get_memory_schemas(_ctx())


@pytest.mark.asyncio
async def test_prefetch_seeds_recursive_listing_and_lets_model_explore():
    ctx = _ctx()
    provider = ConsolidationExtractContextProvider(
        memory_type="entities",
        target_directory="viking://user/u1/memories/entities",
    )
    provider._ctx = ctx

    directory = "viking://user/u1/memories/entities"
    # glob backs the recursive ls: return files across subdirectories.
    glob_result = {
        "matches": [
            {"uri": f"{directory}/person/alice.md", "isDir": False, "size": 100},
            {"uri": f"{directory}/media/book.md", "isDir": False, "size": 200},
            {"uri": f"{directory}/person", "isDir": True, "size": 0},
            {"uri": f"{directory}/.overview.md", "isDir": False, "size": 50},
        ],
        "count": 4,
    }
    provider._viking_fs = SimpleNamespace(glob=AsyncMock(return_value=glob_result))

    messages = await provider.prefetch()

    # A recursive ls seed is added as a tool-call pair, then a user instruction.
    seeded = "\n".join(str(m.get("content", "")) for m in messages)
    assert "person/alice.md" in seeded
    assert "media/book.md" in seeded
    # Reserved overview files are filtered out of the listing.
    assert ".overview.md" not in seeded
    assert messages[-1]["role"] == "user"
    assert "recursive listing" in messages[-1]["content"]
    assert "consolidation operations" in messages[-1]["content"]


def test_provider_exposes_ls_search_read_tools():
    provider = ConsolidationExtractContextProvider(memory_type="entities")
    assert provider.get_tools() == ["ls", "search", "read"]


def test_instruction_mentions_type_and_explore_tools():
    provider = ConsolidationExtractContextProvider(
        memory_type="entities", instruction="Only touch pets."
    )
    text = provider.instruction()
    assert "entities" in text
    assert "recursive=true" in text
    assert "read" in text
    assert "no write tool" in text
    assert "Only touch pets." in text
