#!/usr/bin/env python3
"""
OpenViking memory-mode compile 集成脚本 — 用户: 小美

用 `--skill memory` 对某个记忆类型目录做「就地整理」（dedup/merge/reorganize），
产物严格遵守该记忆类型的 schema。脚本流程：

  Phase 0 (可选 --ingest): 复用小美对话灌一遍数据，产生 entities 等记忆
  Phase 1: 打印整理前该目录下的记忆文件
  Phase 2: 触发 compile(skill="memory") 并轮询 cmp_ 任务直到完成
  Phase 3: 打印整理后的记忆文件，对比前后

用法:
  python tests/integration/test_compile_memory_xiaomei.py --url http://localhost:1933
  python tests/integration/test_compile_memory_xiaomei.py --url http://localhost:1933 --ingest
  python tests/integration/test_compile_memory_xiaomei.py --url http://localhost:1933 \
      --to viking://user/xiaomei/memories/entities \
      --instruction "合并重复实体，但不同实体不要合并"
"""

import argparse
import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import openviking as ov

try:
    from openviking_live_auth import API_KEY_HELP, resolve_api_key
except ModuleNotFoundError:  # pytest/package import path
    from tests.integration.openviking_live_auth import API_KEY_HELP, resolve_api_key

try:
    from test_compressor_v2_xiaomei import _run_ingest_async
except ModuleNotFoundError:  # pytest/package import path
    from tests.integration.test_compressor_v2_xiaomei import _run_ingest_async

# ── 常量 ───────────────────────────────────────────────────────────────────

DISPLAY_NAME = "小美"
DEFAULT_URL = "http://localhost:1933"
PANEL_WIDTH = 78
DEFAULT_API_KEY = None
DEFAULT_ACCOUNT = "default"
DEFAULT_USER = "xiaomei"
DEFAULT_SESSION_ID = "xiaomei-compile-demo"


@dataclass
class CompileCase:
    """一个 compile 集成 case：种子对话 + 目标类型 + 整理指令。

    sessions: 每个内层 list 是一段对话，会各自独立 commit（不同抽取批次），
    以便按需要让抽取产出独立实体。
    """

    key: str
    memory_type: str
    instruction: str
    sessions: list[list[dict]]
    description: str
    rename_source_prefix: str | None = None
    rename_target_prefix: str | None = None
    required_facts: tuple[tuple[str, ...], ...] = ()


# A. 合并（merge）：两段独立对话，同一个人两个不同称呼（阿珍 / 陈静娴）。
# 名字不同、从不点破，抽取阶段拆不开 → 产出两个实体；compile 收到点破指令后合并。
CASE_MERGE = CompileCase(
    key="merge",
    memory_type="entities",
    instruction=(
        "记忆库里的「阿珍」和「陈静娴」其实是同一个人（阿珍是陈静娴的昵称，是小美的大学室友）。"
        "请把这两个实体合并成一个，规范名用「陈静娴」，并保留两条记忆里的全部独立事实"
        "（大学室友、仗义借钱、上海 UI 设计师、下月来访）。其它不同的实体不要合并。"
    ),
    sessions=[
        [
            {
                "user": "我大学室友大家都叫她阿珍，人特别仗义，上次我手头紧她二话不说借钱给我应急。",
                "assistant": "阿珍真是个靠谱的朋友，这样仗义的室友很难得。",
            }
        ],
        [
            {
                "user": "我有个朋友叫陈静娴，在上海一家互联网公司做 UI 设计师，下个月要来找我玩。",
                "assistant": "陈静娴听起来很厉害呀，做 UI 设计的，下个月见面一定很开心。",
            }
        ],
    ],
    description="两个不同称呼实为同一人，compile 合并（delete 一个 + update 另一个）。",
)

# B. 拆分（split）：两个同名不同人（都叫「小林」）。因为实体 URI = category/name，
# 同名会落到同一文件，抽取被迫把两人塞进一个 person/小林.md → compile 拆成两个。
CASE_SPLIT = CompileCase(
    key="split",
    memory_type="entities",
    instruction=(
        "记忆库里的「小林」其实混进了两个不同的人：一个是小美市场部的同事小林，"
        "另一个是小美的健身教练小林。请把它们拆成两个独立实体，分别保留各自的事实，"
        "用能区分二者的名字（例如「同事小林」和「教练小林」）。不要把不同的人合在一起。"
    ),
    sessions=[
        [
            {
                "user": "我同事小林在市场部，平时特别爱开玩笑，办公室气氛全靠他活跃。",
                "assistant": "有这样的同事上班一定很快乐，小林听起来很会调节气氛。",
            },
            {
                "user": "另外我健身房的教练也叫小林，特别专业，帮我把深蹲动作纠正过来了。",
                "assistant": "教练小林很专业呀，动作规范太重要了，能避免受伤。",
            },
        ],
    ],
    description="同名两人被抽取塞进一个文件，compile 拆分（add 新实体 + update/delete 原文件）。",
)

# C. 精简去重（dedup/edit）：多段对话反复用不同措辞说同一个人的同一件事，
# 抽取累积出啰嗦/重复的正文 → compile 原地 edit/drop 精简（文件变小，不删不增）。
CASE_DEDUP = CompileCase(
    key="dedup",
    memory_type="entities",
    instruction=(
        "记忆库里关于「大壮」的记忆有很多重复、啰嗦、同义反复的表述。请在不丢失任何"
        "独立事实的前提下，原地精简去重，让内容更紧凑。不要合并或删除其它实体。"
    ),
    sessions=[
        [
            {
                "user": "我发小大壮人特别好，真的超级好，对朋友特别特别够意思，是个非常仗义的人。",
                "assistant": "大壮听起来是个很重情义的发小。",
            }
        ],
        [
            {
                "user": "大壮真的很讲义气，对哥们儿没话说，特别仗义，谁有事他都第一个到。",
                "assistant": "这样重义气的朋友很难得，遇到事有他在很安心。",
            }
        ],
    ],
    description="同一实体反复重复陈述，compile 原地精简（update，文件变小）。",
)

# D. 改名（rename）：先抽取中文实体名，再由 compile 修改 URI identity fields，
# 验证表现为 add 英文 URI + delete 中文 URI，同时保留全部事实与关系链接。
CASE_RENAME = CompileCase(
    key="rename",
    memory_type="entities",
    instruction=(
        "把人物「{source_name}」的目录和文件名改成英文：目录用 person，文件名用"
        " {target_name}.md。保留摄影老师、杭州、风光摄影和十月去西湖练习长曝光等事实；"
        "不要创建重复实体或修改其他实体。"
    ),
    sessions=[
        [
            {
                "user": (
                    "我的摄影老师叫{source_name}，她住在杭州，专门拍风光摄影。"
                    "她十月会带我去西湖练习长曝光。"
                ),
                "assistant": (
                    "{source_name}老师的风光摄影经验很丰富，十月去西湖练习长曝光很值得期待。"
                ),
            }
        ]
    ],
    description=(
        "目录和文件名在中英文之间往返改名，每一步均为 add 新 URI + delete 旧 URI，并迁移关系链接。"
    ),
    rename_source_prefix="罗晴",
    rename_target_prefix="photo_teacher",
    required_facts=(
        ("摄影老师", "photography teacher"),
        ("杭州", "hangzhou"),
        ("风光摄影", "landscape photography"),
        ("十月", "october"),
        ("西湖", "west lake"),
        ("长曝光", "long exposure"),
    ),
)

# E. 换 memory type：整理 preferences，而不是 entities，验证 compile 对不同 schema 都工作。
CASE_PREFERENCES = CompileCase(
    key="preferences",
    memory_type="preferences",
    instruction=(
        "整理小美的偏好记忆：合并明显重复或矛盾的偏好，保留每一条独立的偏好事实，"
        "让偏好集合清晰、无冗余。不同维度的偏好不要强行合并。"
    ),
    sessions=[
        [
            {
                "user": "我吃麻辣烫喜欢多放醋和麻酱，另外我不太能吃辣，最好少辣或者微辣。",
                "assistant": "记住啦，多醋多麻酱、少辣，这样的麻辣烫更合你口味。",
            }
        ],
        [
            {
                "user": "再补充一下，我吃东西真的不能太辣，微辣就是我的极限，麻酱一定要多给。",
                "assistant": "好的，微辣上限、麻酱多多，帮你记牢了。",
            }
        ],
    ],
    description="整理 preferences 类型，验证 compile 对非 entities schema 同样工作。",
)

CASES: dict[str, CompileCase] = {
    CASE_MERGE.key: CASE_MERGE,
    CASE_SPLIT.key: CASE_SPLIT,
    CASE_DEDUP.key: CASE_DEDUP,
    CASE_RENAME.key: CASE_RENAME,
    CASE_PREFERENCES.key: CASE_PREFERENCES,
}

console = Console()


def _memory_dir(user: str, memory_type: str) -> str:
    return f"viking://user/{user}/memories/{memory_type}"


def _unique_rename_names(case: CompileCase) -> tuple[str, str]:
    token = uuid.uuid4().hex[:8]
    surnames = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨"
    given_names = "清岚若晴雨桐诗涵婉宁静怡思远知夏"
    source_name = (
        surnames[int(token[:2], 16) % len(surnames)]
        + given_names[int(token[2:4], 16) % len(given_names)]
        + given_names[int(token[4:6], 16) % len(given_names)]
        + given_names[int(token[6:8], 16) % len(given_names)]
    )
    return (
        source_name,
        f"{case.rename_target_prefix}_{token}",
    )


# ── 为一个 case 追加种子对话（每段独立 commit），让抽取产生实体 ───────────────


async def _ingest_case_sessions(
    client,
    case: CompileCase,
    wait_seconds: float,
    template_values: dict[str, str] | None = None,
) -> None:
    """为 case 的每段对话独立开 session 并 commit，让抽取各自产出实体。"""
    base_time = datetime(2023, 4, 9, 20, 0)
    for idx, turns in enumerate(case.sessions):
        session = await client.create_session()
        session_id = session.get("session_id")
        console.print(f"  Session[{idx + 1}]: [bold cyan]{session_id}[/bold cyan]")

        # 每段对话给不同的会话时间，进一步区分抽取批次。
        session_time_str = base_time.replace(day=base_time.day + idx).isoformat()
        for turn in turns:
            user_text = turn["user"]
            assistant_text = turn["assistant"]
            if template_values:
                user_text = user_text.format(**template_values)
                assistant_text = assistant_text.format(**template_values)
            await client.add_message(
                session_id,
                role="user",
                parts=[{"type": "text", "text": user_text}],
                options={"created_at": session_time_str},
            )
            await client.add_message(
                session_id,
                role="assistant",
                parts=[{"type": "text", "text": assistant_text}],
                options={"created_at": session_time_str},
            )

        console.print(f"  [yellow]提交种子对话 Session[{idx + 1}]（触发记忆抽取）...[/yellow]")
        commit_result = await client.commit_session(session_id)
        task_id = commit_result.get("task_id")
        console.print(f"  [bold cyan]trace_id: {commit_result.get('trace_id')}[/bold cyan]")
        if task_id:
            while True:
                task = await client.get_task(task_id)
                if not task or task.get("status") in ("completed", "failed"):
                    break
                await asyncio.sleep(1)
            console.print(f"  [green]抽取 {task.get('status') if task else 'unknown'}[/green]")
        await client.wait_processed()
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)


# ── 列出目录下的记忆文件 ─────────────────────────────────────────────────────


async def _snapshot_memory_dir(client, directory: str) -> list[dict]:
    """递归列出某记忆目录下的 .md 文件（跳过 .overview.md / .abstract.md）。"""
    files: list[dict] = []
    try:
        entries = await client.ls(
            directory,
            recursive=True,
            output="original",
            show_all_hidden=False,
            node_limit=1000,
        )
    except Exception as e:
        console.print(f"    [red]列目录失败 {directory}: {e}[/red]")
        return files

    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("isDir"):
            continue
        uri = str(entry.get("uri", ""))
        name = str(entry.get("name", ""))
        if not uri.endswith(".md"):
            continue
        if name in {".overview.md", ".abstract.md"} or uri.endswith(
            ("/.overview.md", "/.abstract.md")
        ):
            continue
        files.append(entry)
    return files


def _render_snapshot(title: str, directory: str, files: list[dict]) -> None:
    table = Table(title=title, box=box.ROUNDED, show_header=True, header_style="bold")
    table.add_column("#", style="bold", width=4)
    table.add_column("URI", style="cyan")
    table.add_column("size", justify="right", width=8)
    for i, entry in enumerate(files, 1):
        uri = str(entry.get("uri", ""))
        rel = uri.replace(directory.rstrip("/") + "/", "")
        table.add_row(str(i), rel, str(entry.get("size", "")))
    console.print(table)
    console.print(f"  共 [bold]{len(files)}[/bold] 个记忆文件")


def _render_change_lists(result: dict) -> None:
    """打印整理产生的 adds/updates/deletes 文件名清单（不含内容）。"""
    table = Table(title="变化文件清单", box=box.ROUNDED, show_header=True, header_style="bold")
    table.add_column("类型", style="bold", width=10)
    table.add_column("URI", style="cyan")
    for kind, style in (("adds", "green"), ("updates", "yellow"), ("deletes", "red")):
        for uri in result.get(kind, []) or []:
            table.add_row(f"[{style}]{kind}[/{style}]", str(uri))
    console.print(table)
    console.print(
        f"  新增 [green]{result.get('total_adds', 0)}[/green] / "
        f"修改 [yellow]{result.get('total_updates', 0)}[/yellow] / "
        f"删除 [red]{result.get('total_deletes', 0)}[/red]"
    )
    errors = result.get("errors") or []
    if errors:
        console.print(f"  [red]errors: {errors}[/red]")


def _entry_uri(entry: dict) -> str:
    return str(entry.get("uri", ""))


async def _verify_rename_case(
    client,
    *,
    case: CompileCase,
    directory: str,
    source_uri: str,
    target_uri: str,
    task: dict,
    after: list[dict],
) -> None:
    """Verify rename semantics from task diff through persisted content and links."""
    result = task.get("result") or {}
    errors = result.get("errors") or []
    if task.get("status") != "completed" or errors:
        raise AssertionError(
            f"rename compile 未成功完成: status={task.get('status')}, errors={errors}"
        )

    adds = set(result.get("adds") or [])
    deletes = set(result.get("deletes") or [])
    if target_uri not in adds or source_uri not in deletes:
        raise AssertionError(
            "rename diff 不符合 add(new)+delete(old): "
            f"expected add={target_uri}, delete={source_uri}, result={result}"
        )

    after_uris = {_entry_uri(entry) for entry in after}
    if source_uri in after_uris or target_uri not in after_uris:
        raise AssertionError(
            "rename 后目录状态不正确: "
            f"source_exists={source_uri in after_uris}, target_exists={target_uri in after_uris}"
        )

    target_content = await client.read(target_uri)
    normalized_content = target_content.casefold()
    missing_facts = [
        alternatives
        for alternatives in case.required_facts
        if not any(term.casefold() in normalized_content for term in alternatives)
    ]
    if missing_facts:
        raise AssertionError(f"改名目标文件丢失事实: {missing_facts}\n{target_content}")

    stale_markers = {source_uri, source_uri.removeprefix("viking://user/")}
    source_rel = source_uri.removeprefix(directory.rstrip("/") + "/")
    stale_markers.add(f"entities/{source_rel}")
    for updated_uri in [target_uri, *(result.get("updates") or [])]:
        updated_content = await client.read_raw(updated_uri)
        found_markers = [marker for marker in stale_markers if marker in updated_content]
        if found_markers:
            raise AssertionError(f"关系迁移后仍残留旧 URI/href: {updated_uri} -> {found_markers}")

    console.print(f"  [bold green]rename 验证通过:[/bold green] {source_uri} → {target_uri}")


async def _find_entity_uri_by_content(
    client, directory: str, files: list[dict], source_name: str
) -> str:
    candidates = []
    for entry in files:
        uri = _entry_uri(entry)
        relative_uri = uri.removeprefix(directory.rstrip("/") + "/")
        if not relative_uri.startswith(("person/", "人物/")):
            continue
        content = await client.read(uri)
        if source_name in content:
            candidates.append(uri)
    if len(candidates) != 1:
        raise AssertionError(
            f"rename case 需要按内容找到恰好一个人物「{source_name}」，实际: {candidates}"
        )
    return candidates[0]


async def _run_rename_step(
    client,
    args,
    case: CompileCase,
    *,
    directory: str,
    label: str,
    source_uri: str,
    target_uri: str,
    instruction: str,
) -> list[dict]:
    console.rule(f"[bold]Rename {label}: {source_uri} → {target_uri}[/bold]")
    before = await _snapshot_memory_dir(client, directory)
    before_uris = {_entry_uri(entry) for entry in before}
    if source_uri not in before_uris or target_uri in before_uris:
        raise AssertionError(
            f"rename {label} 前置状态错误: source_exists={source_uri in before_uris}, "
            f"target_exists={target_uri in before_uris}"
        )

    task = await _run_memory_compile(client, directory, instruction)
    result = task.get("result") or {}
    trace_id = result.get("trace_id") or task.get("trace_id") or ""
    if trace_id:
        console.print(f"  [bold cyan]trace_id: {trace_id}[/bold cyan]")
    _render_change_lists(result)

    await client.wait_processed()
    if args.wait > 0:
        await asyncio.sleep(args.wait)
    after = await _snapshot_memory_dir(client, directory)
    await _verify_rename_case(
        client,
        case=case,
        directory=directory,
        source_uri=source_uri,
        target_uri=target_uri,
        task=task,
        after=after,
    )
    return after


async def _run_rename_round_trip(
    client,
    args,
    case: CompileCase,
    *,
    directory: str,
    source_name: str,
    english_name: str,
    initial: list[dict],
) -> None:
    initial_uri = await _find_entity_uri_by_content(client, directory, initial, source_name)
    english_uri = f"{directory.rstrip('/')}/person/{english_name}.md"
    chinese_uri = f"{directory.rstrip('/')}/人物/{source_name}.md"

    to_english = case.instruction.format(
        source_name=source_name,
        target_name=english_name,
    )
    await _run_rename_step(
        client,
        args,
        case,
        directory=directory,
        label="initial → English",
        source_uri=initial_uri,
        target_uri=english_uri,
        instruction=to_english,
    )

    to_chinese = (
        f"把英文名为「{english_name}」的人物改回中文名「{source_name}」，同时把分类目录改成"
        f"中文“人物”。保留摄影老师、杭州、风光摄影和十月去西湖练习长曝光等事实；"
        f"不要创建重复实体或修改其他实体。"
    )
    await _run_rename_step(
        client,
        args,
        case,
        directory=directory,
        label="English → Chinese",
        source_uri=english_uri,
        target_uri=chinese_uri,
        instruction=to_chinese,
    )

    final = await _run_rename_step(
        client,
        args,
        case,
        directory=directory,
        label="Chinese → English",
        source_uri=chinese_uri,
        target_uri=english_uri,
        instruction=to_english,
    )

    chinese_directory = chinese_uri.rpartition("/")[0]
    try:
        await client.stat(chinese_directory)
    except Exception as exc:
        if getattr(exc, "code", None) != "NOT_FOUND":
            raise
    else:
        raise AssertionError(f"中文源目录在最后一个文件迁出后仍然存在: {chinese_directory}")

    console.rule(f"[bold]Rename round trip 最终状态 — {directory}[/bold]")
    _render_snapshot("往返改名后", directory, final)
    console.print(
        Panel(
            f"[bold]case:[/bold] {case.key}\n"
            f"[bold]初始 URI:[/bold] {initial_uri}\n"
            f"[bold]英文 URI:[/bold] {english_uri}\n"
            f"[bold]中文 URI:[/bold] {chinese_uri}\n"
            f"[bold]中文目录清理:[/bold] {chinese_directory} 已删除\n"
            "[bold green]往返三步全部验证通过[/bold green]",
            title="对比 — rename round trip",
            style="magenta",
            width=PANEL_WIDTH,
        )
    )


# ── Phase 2: 触发 memory compile 并轮询 ─────────────────────────────────────


async def _run_memory_compile(client, directory: str, instruction: str) -> dict:
    console.print()
    console.print(f"  [yellow]触发 compile(skill=memory)... to={directory}[/yellow]")
    options = {"instruction": instruction} if instruction else None
    accepted = await client.compile(
        from_uris=[],  # memory 模式不需要 --from，就地整理 --to
        to=directory,
        skill="memory",
        options=options,
    )
    console.print(f"  Accepted: {accepted}")
    # client.compile 走 _handle_response，已经取过 .get("result")，
    # 所以 accepted 通常就是 result 本身；兜底再剥一层。
    result = accepted if isinstance(accepted, dict) else {}
    if isinstance(result.get("result"), dict):
        result = result["result"]
    task_id = result.get("task_id") or result.get("resource_id")

    if not task_id:
        console.print("  [red]未拿到 task_id，无法轮询[/red]")
        return {}

    console.print(f"  [yellow]等待整理完成 (task_id={task_id})...[/yellow]")
    now = time.time()
    task = None
    while True:
        task = await client.get_task(task_id)
        if not task or task.get("status") in ("completed", "failed", "cancelled"):
            break
        await asyncio.sleep(1)
    elapsed = time.time() - now
    status = task.get("status", "unknown") if task else "not found"
    console.print(f"  [green]任务 {status}，耗时 {elapsed:.2f}s[/green]")
    console.print(f"  Task 详情: {task}")
    return task or {}


# ── 入口 ───────────────────────────────────────────────────────────────────


async def _run_case(client, args, case: CompileCase) -> None:
    """跑单个 compile case：种子对话 → 整理前快照 → compile → 整理后对比。"""
    directory = args.to or _memory_dir(args.user, case.memory_type)
    source_name = None
    target_name = None
    if case.rename_target_prefix:
        source_name, target_name = _unique_rename_names(case)

    console.rule(f"[bold magenta]CASE: {case.key} — {case.description}[/bold magenta]")

    console.rule(f"[bold]Phase 0b: 种子对话 case={case.key}[/bold]")
    await _ingest_case_sessions(
        client,
        case,
        wait_seconds=args.wait,
        template_values={"source_name": source_name, "target_name": target_name}
        if source_name and target_name
        else None,
    )

    console.rule(f"[bold]Phase 1: 整理前 — {directory}[/bold]")
    before = await _snapshot_memory_dir(client, directory)
    _render_snapshot("整理前", directory, before)
    if source_name and target_name:
        await _run_rename_round_trip(
            client,
            args,
            case,
            directory=directory,
            source_name=source_name,
            english_name=target_name,
            initial=before,
        )
        return

    console.rule("[bold]Phase 2: 执行 memory compile[/bold]")
    task = await _run_memory_compile(client, directory, case.instruction)

    result = task.get("result") or {}
    trace_id = result.get("trace_id") or task.get("trace_id") or ""
    if trace_id:
        console.print(f"  [bold cyan]trace_id: {trace_id}[/bold cyan]")

    if task.get("status") == "completed":
        _render_change_lists(result)
    elif task.get("status") == "failed":
        console.print(f"  [red]整理失败: {task.get('error')}[/red]")

    # 等待向量化/overview 刷新
    await client.wait_processed()
    if args.wait > 0:
        await asyncio.sleep(args.wait)

    console.rule(f"[bold]Phase 3: 整理后 — {directory}[/bold]")
    after = await _snapshot_memory_dir(client, directory)
    _render_snapshot("整理后", directory, after)

    console.print()
    console.print(
        Panel(
            f"[bold]case:[/bold] {case.key}\n"
            f"[bold]整理前:[/bold] {len(before)} 个文件\n"
            f"[bold]整理后:[/bold] {len(after)} 个文件\n"
            f"[bold]净变化:[/bold] {len(after) - len(before):+d}",
            title=f"对比 — {case.key}",
            style="magenta",
            width=PANEL_WIDTH,
        )
    )


async def _main_async(args) -> None:
    client = ov.AsyncHTTPClient(
        url=args.url,
        api_key=resolve_api_key(args.api_key),
        account=args.account,
        user=args.user,
        timeout=600,
    )

    # 选择要跑的 case：--case all 跑全部，否则跑指定的一个。
    if args.case == "all":
        selected = list(CASES.values())
    else:
        selected = [CASES[args.case]]

    try:
        await client.initialize()
        console.print(f"  [green]已连接[/green] {args.url}")

        if args.ingest:
            console.rule("[bold]Phase 0: 灌入小美对话数据[/bold]")
            await _run_ingest_async(client, session_id=args.session_id, wait_seconds=args.wait)

        for case in selected:
            await _run_case(client, args, case)

    except Exception as e:
        console.print(Panel(f"[bold red]Error:[/bold red] {e}", style="red", width=PANEL_WIDTH))
        import traceback

        traceback.print_exc()
        raise

    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(
        description=f"OpenViking memory-mode compile 集成脚本 — {DISPLAY_NAME}"
    )
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Server URL (默认: {DEFAULT_URL})")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help=API_KEY_HELP)
    parser.add_argument(
        "--account", default=DEFAULT_ACCOUNT, help=f"OpenViking account (默认: {DEFAULT_ACCOUNT})"
    )
    parser.add_argument(
        "--user", default=DEFAULT_USER, help=f"OpenViking user (默认: {DEFAULT_USER})"
    )
    parser.add_argument(
        "--to",
        default=None,
        help="直接指定要整理的记忆目录 URI（覆盖 case 的 memory_type）。",
    )
    parser.add_argument(
        "--ingest",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="整理前先灌一遍小美对话数据 (默认: 是；用 --no-ingest 关闭)。",
    )
    parser.add_argument(
        "--case",
        choices=["all", *sorted(CASES.keys())],
        default="all",
        help=(
            "compile 集成 case（默认: all，依次跑全部）。"
            "merge=合并两个同人不同称呼；split=拆分同名两人；"
            "dedup=原地精简重复表述；rename=目录和文件名中英文往返；"
            "preferences=整理 preferences 类型。"
        ),
    )
    parser.add_argument(
        "--session-id", default=DEFAULT_SESSION_ID, help=f"Session ID (默认: {DEFAULT_SESSION_ID})"
    )
    parser.add_argument("--wait", type=float, default=5.0, help="每阶段后额外等待秒数 (默认: 5)")
    args = parser.parse_args()

    console.print(
        Panel(
            f"[bold]OpenViking memory compile — {DISPLAY_NAME}[/bold]\n"
            f"Server: {args.url}  |  case: {args.case}  |  ingest: {args.ingest}",
            style="magenta",
            width=PANEL_WIDTH,
        )
    )

    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()
