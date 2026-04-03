from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

CODEX_CORE_TOOLS = {
    "exec_command",
    "write_stdin",
    "update_plan",
    "request_user_input",
    "view_image",
    "spawn_agent",
    "send_input",
    "resume_agent",
    "wait",
    "close_agent",
    "spawn_agents_on_csv",
    "apply_patch",
    "parallel",
}

CLAUDE_EDIT_TOOLS = {"Write", "Edit", "MultiEdit"}
CLAUDE_BUILTIN_TOOLS = {
    "Bash",
    "Read",
    "Write",
    "Edit",
    "MultiEdit",
    "Grep",
    "Glob",
    "WebSearch",
    "TodoWrite",
    "NotebookRead",
    "NotebookEdit",
}


@dataclass
class SessionStats:
    source: str
    session_id: str
    project_path: str = ""
    project_name: str = "unknown"
    first_ts: Any = None
    last_ts: Any = None
    user_messages: int = 0
    assistant_messages: int = 0
    tool_calls_total: int = 0
    mcp_calls: int = 0
    code_lines_written: int = 0
    files_touched: set[str] = field(default_factory=set)
    mcp_tools: Counter = field(default_factory=Counter)
    tool_counts: Counter = field(default_factory=Counter)
    # New metrics
    subagent_count: int = 0
    thinking_blocks: int = 0
    turn_durations_ms: list[int] = field(default_factory=list)
    rate_limit_max_pct: float = 0.0
    rate_limit_daily_max_pct: float = 0.0


from dashboard import iter_jsonl as _iter_jsonl


def _project_name(project_path: str, source_path: Path) -> str:
    if project_path:
        name = Path(project_path).name.strip()
        if name:
            return name
    parts = source_path.parts
    if "projects" in parts:
        idx = parts.index("projects")
        if idx + 1 < len(parts):
            candidate = parts[idx + 1]
            if candidate:
                return candidate
    return "unknown"


def _count_lines(text: str) -> int:
    if not text:
        return 0
    return len(text.splitlines())


def _extract_text_items(content: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(content, str):
        value = content.strip()
        if value:
            texts.append(value)
        return texts

    if not isinstance(content, list):
        return texts

    for item in content:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type in {"input_text", "output_text", "text"}:
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
    return texts


def _parse_apply_patch_args(arguments: Any) -> tuple[int, set[str]]:
    raw = arguments
    if isinstance(arguments, str):
        try:
            decoded = json.loads(arguments)
            if isinstance(decoded, str):
                raw = decoded
            elif isinstance(decoded, dict):
                raw = decoded.get("patch") or decoded.get("input") or arguments
        except json.JSONDecodeError:
            raw = arguments

    if not isinstance(raw, str):
        raw = str(raw)

    files: set[str] = set()
    lines_written = 0

    for line in raw.splitlines():
        if line.startswith("*** Add File: "):
            files.add(line.replace("*** Add File: ", "", 1).strip())
            continue
        if line.startswith("*** Update File: "):
            files.add(line.replace("*** Update File: ", "", 1).strip())
            continue
        if line.startswith("*** Delete File: "):
            files.add(line.replace("*** Delete File: ", "", 1).strip())
            continue
        if line.startswith("+") and not line.startswith("+++"):
            lines_written += 1

    return lines_written, files


def _decode_json_like(value: Any) -> Any:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded
        except json.JSONDecodeError:
            return value
    return value


def _extract_target_files_from_payload(tool_name: str, payload: Any) -> set[str]:
    files: set[str] = set()
    decoded = _decode_json_like(payload)
    lowered = (tool_name or "").lower()

    if isinstance(decoded, dict):
        for key in ("file_path", "path", "target_file", "filename", "file"):
            value = decoded.get(key)
            if isinstance(value, str) and value.strip():
                files.add(value.strip())

        if lowered == "multiedit":
            edits = decoded.get("edits") or []
            if isinstance(edits, list):
                for edit in edits:
                    if not isinstance(edit, dict):
                        continue
                    value = (
                        edit.get("file_path")
                        or edit.get("path")
                        or edit.get("target_file")
                        or ""
                    )
                    if isinstance(value, str) and value.strip():
                        files.add(value.strip())

    if lowered == "apply_patch":
        _lines, patch_files = _parse_apply_patch_args(payload)
        files.update(patch_files)

    return files


def _infer_tool_success_from_output(output: Any) -> bool | None:
    if output is None:
        return None

    if isinstance(output, (dict, list)):
        text = json.dumps(output, ensure_ascii=False)
    else:
        text = str(output)

    cleaned = text.strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    exit_code_match = re.search(r"process exited with code\s+(-?\d+)", lowered)
    if exit_code_match:
        return int(exit_code_match.group(1)) == 0

    if "\"is_error\":false" in lowered:
        return True
    if "\"is_error\":true" in lowered:
        return False
    if "success. updated the following files" in lowered:
        return True

    failure_tokens = [
        "permission denied",
        "timed out",
        "traceback",
        "failed",
        "exception",
        "operation not permitted",
    ]
    if any(token in lowered for token in failure_tokens):
        return False

    return None


def _extract_query_text_from_payload(tool_name: str, payload: Any) -> str | None:
    lowered = (tool_name or "").lower()
    decoded = _decode_json_like(payload)

    if lowered in {"websearch", "search_query"} and isinstance(decoded, dict):
        if isinstance(decoded.get("query"), str):
            return decoded.get("query")
        if isinstance(decoded.get("q"), str):
            return decoded.get("q")

    if lowered == "search_query" and isinstance(decoded, dict):
        items = decoded.get("search_query")
        if isinstance(items, list):
            queries = []
            for item in items:
                if isinstance(item, dict):
                    q = item.get("q")
                    if isinstance(q, str) and q.strip():
                        queries.append(q.strip())
            if queries:
                return " | ".join(queries)

    if "websearch" in lowered and isinstance(decoded, dict):
        query = decoded.get("query") or decoded.get("q")
        if isinstance(query, str) and query.strip():
            return query.strip()

    if "search" in lowered and isinstance(decoded, dict):
        query = decoded.get("query") or decoded.get("q")
        if isinstance(query, str) and query.strip():
            return query.strip()

    return None


def _is_mcp_codex_tool(name: str) -> bool:
    if not name:
        return False
    if name in CODEX_CORE_TOOLS:
        return False
    lowered = name.lower().strip()
    if lowered.startswith("mcp"):
        return True
    return re.search(r"(^|[._:/-])mcp([._:/-]|$)", lowered) is not None


def _is_mcp_claude_tool(name: str) -> bool:
    if not name:
        return False
    if name in CLAUDE_BUILTIN_TOOLS:
        return False
    lowered = name.lower()
    return lowered.startswith("mcp") or "mcp" in lowered or "mcp__" in name


def _session_key(source: str, session_id: str) -> tuple[str, str]:
    return source, session_id


def _get_or_create_stats(
    store: dict[tuple[str, str], SessionStats],
    source: str,
    session_id: str,
    project_path: str,
    file_path: Path,
) -> SessionStats:
    key = _session_key(source, session_id)
    if key not in store:
        store[key] = SessionStats(
            source=source,
            session_id=session_id,
            project_path=project_path,
            project_name=_project_name(project_path, file_path),
        )
    stats = store[key]
    if project_path and not stats.project_path:
        stats.project_path = project_path
        stats.project_name = _project_name(project_path, file_path)
    return stats


def _update_time_window(stats: SessionStats, timestamp: Any) -> None:
    if timestamp is None:
        return
    if stats.first_ts is None or timestamp < stats.first_ts:
        stats.first_ts = timestamp
    if stats.last_ts is None or timestamp > stats.last_ts:
        stats.last_ts = timestamp


def _parse_codex_psyco(
    codex_root: Path,
) -> tuple[dict[tuple[str, str], SessionStats], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    stats_store: dict[tuple[str, str], SessionStats] = {}
    chat_rows: list[dict[str, Any]] = []
    tool_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    tool_row_index_by_call: dict[tuple[str, str], int] = {}

    for file_path in sorted(codex_root.rglob("*.jsonl")) if codex_root.exists() else []:
        session_id = file_path.stem
        project_path = ""

        try:
            for item in _iter_jsonl(file_path):
                timestamp = pd.to_datetime(item.get("timestamp"), utc=True, errors="coerce")
                if pd.isna(timestamp):
                    timestamp = None

                event_type = item.get("type")
                payload = item.get("payload") or {}

                if event_type == "session_meta":
                    session_id = payload.get("id") or session_id
                    project_path = payload.get("cwd") or project_path
                    stats = _get_or_create_stats(
                        stats_store, "codex", session_id, project_path, file_path
                    )
                    _update_time_window(stats, timestamp)
                    continue

                if event_type == "turn_context":
                    project_path = payload.get("cwd") or project_path
                    stats = _get_or_create_stats(
                        stats_store, "codex", session_id, project_path, file_path
                    )
                    _update_time_window(stats, timestamp)
                    continue

                # Extract rate limits and turn durations from event_msg
                if event_type == "event_msg":
                    sub_type = payload.get("type")
                    if sub_type == "token_count":
                        rate_limits = payload.get("rate_limits") or {}
                        primary_pct = rate_limits.get("primary", {}).get("used_percent", 0) or 0
                        secondary_pct = rate_limits.get("secondary", {}).get("used_percent", 0) or 0
                        stats.rate_limit_max_pct = max(stats.rate_limit_max_pct, float(primary_pct))
                        stats.rate_limit_daily_max_pct = max(stats.rate_limit_daily_max_pct, float(secondary_pct))
                    continue

                if event_type != "response_item":
                    continue

                stats = _get_or_create_stats(
                    stats_store, "codex", session_id, project_path, file_path
                )
                _update_time_window(stats, timestamp)

                payload_type = payload.get("type")
                if payload_type == "message":
                    role = payload.get("role")
                    if role not in {"user", "assistant"}:
                        continue
                    texts = _extract_text_items(payload.get("content"))
                    if role == "user" and texts:
                        stats.user_messages += 1
                    if role == "assistant" and texts:
                        stats.assistant_messages += 1
                    for text in texts:
                        chat_rows.append(
                            {
                                "source": "codex",
                                "timestamp": timestamp,
                                "session_id": session_id,
                                "project_name": stats.project_name,
                                "role": role,
                                "text": text,
                            }
                        )
                    continue

                if payload_type == "function_call":
                    name = payload.get("name") or "unknown"
                    stats.tool_calls_total += 1
                    stats.tool_counts[name] += 1

                    is_mcp = _is_mcp_codex_tool(name)
                    if is_mcp:
                        stats.mcp_calls += 1
                        stats.mcp_tools[name] += 1

                    arguments = payload.get("arguments")
                    query_text = _extract_query_text_from_payload(name, arguments)
                    target_files = _extract_target_files_from_payload(name, arguments)
                    if name == "apply_patch":
                        lines_written, touched_files = _parse_apply_patch_args(arguments)
                        stats.code_lines_written += lines_written
                        stats.files_touched.update(touched_files)
                        target_files.update(touched_files)

                    if target_files:
                        stats.files_touched.update(target_files)

                    call_id = str(payload.get("call_id") or "")

                    tool_rows.append(
                        {
                            "source": "codex",
                            "timestamp": timestamp,
                            "session_id": session_id,
                            "project_name": stats.project_name,
                            "tool_name": name,
                            "is_mcp": is_mcp,
                            "query_text": query_text,
                            "target_file": ", ".join(sorted(target_files)) if target_files else "",
                            "call_id": call_id,
                            "success": None,
                            "error_hint": "",
                        }
                    )
                    if call_id:
                        tool_row_index_by_call[(session_id, call_id)] = len(tool_rows) - 1
                    continue

                if payload_type == "function_call_output":
                    call_id = str(payload.get("call_id") or "")
                    output = payload.get("output")
                    success = _infer_tool_success_from_output(output)
                    error_hint = ""
                    if success is False:
                        error_hint = str(output or "").replace("\n", " ").strip()[:220]
                    if call_id and (session_id, call_id) in tool_row_index_by_call:
                        row_idx = tool_row_index_by_call[(session_id, call_id)]
                        tool_rows[row_idx]["success"] = success
                        if error_hint:
                            tool_rows[row_idx]["error_hint"] = error_hint
        except Exception as exc:  # pragma: no cover - defensive for mixed historical logs
            warnings.append(f"Failed to parse Codex session file {file_path}: {exc}")

    return stats_store, chat_rows, tool_rows, warnings


def _parse_claude_psyco(
    claude_root: Path,
    stats_store: dict[tuple[str, str], SessionStats],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    chat_rows: list[dict[str, Any]] = []
    tool_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    tool_row_index_by_call: dict[tuple[str, str], int] = {}

    for file_path in sorted(claude_root.rglob("*.jsonl")) if claude_root.exists() else []:
        try:
            for item in _iter_jsonl(file_path):
                item_type = item.get("type")

                # Extract turn duration from system events
                if item_type == "system":
                    subtype = item.get("subtype")
                    if subtype == "turn_duration":
                        duration_ms = item.get("durationMs")
                        if isinstance(duration_ms, (int, float)) and duration_ms > 0:
                            session_id = item.get("sessionId") or file_path.stem
                            project_path = item.get("cwd") or ""
                            stats = _get_or_create_stats(
                                stats_store, "claude", session_id, project_path, file_path
                            )
                            stats.turn_durations_ms.append(int(duration_ms))
                    continue

                if item_type not in {"user", "assistant"}:
                    continue

                timestamp = pd.to_datetime(item.get("timestamp"), utc=True, errors="coerce")
                if pd.isna(timestamp):
                    timestamp = None

                session_id = item.get("sessionId") or file_path.stem
                project_path = item.get("cwd") or item.get("project") or ""
                stats = _get_or_create_stats(
                    stats_store, "claude", session_id, project_path, file_path
                )
                _update_time_window(stats, timestamp)

                message = item.get("message") or {}

                if item_type == "user":
                    content = message.get("content") or []
                    if isinstance(content, list):
                        for part in content:
                            if not isinstance(part, dict):
                                continue
                            if part.get("type") != "tool_result":
                                continue
                            call_id = str(part.get("tool_use_id") or "")
                            if not call_id:
                                continue
                            row_idx = tool_row_index_by_call.get((session_id, call_id))
                            if row_idx is None:
                                continue
                            is_error = part.get("is_error")
                            success = None
                            if isinstance(is_error, bool):
                                success = not is_error
                            else:
                                success = _infer_tool_success_from_output(part.get("content"))
                            tool_rows[row_idx]["success"] = success
                            if success is False:
                                tool_rows[row_idx]["error_hint"] = str(part.get("content") or "").replace(
                                    "\n", " "
                                ).strip()[:220]

                    texts = _extract_text_items(message.get("content"))
                    if isinstance(message.get("content"), str):
                        texts = [message["content"].strip()] if message["content"].strip() else []
                    if texts:
                        stats.user_messages += 1
                    for text in texts:
                        chat_rows.append(
                            {
                                "source": "claude",
                                "timestamp": timestamp,
                                "session_id": session_id,
                                "project_name": stats.project_name,
                                "role": "user",
                                "text": text,
                            }
                        )
                    continue

                # Detect subagent (sidechain) messages
                if item.get("isSidechain") and item.get("agentId"):
                    # Count unique agentIds as subagent spawns
                    agent_id = item.get("agentId")
                    if agent_id and not hasattr(stats, "_seen_agents"):
                        stats._seen_agents = set()  # type: ignore[attr-defined]
                    if agent_id and hasattr(stats, "_seen_agents"):
                        if agent_id not in stats._seen_agents:  # type: ignore[attr-defined]
                            stats._seen_agents.add(agent_id)  # type: ignore[attr-defined]
                            stats.subagent_count += 1

                content = message.get("content") or []
                text_found = False
                if isinstance(content, list):
                    for part in content:
                        if not isinstance(part, dict):
                            continue
                        # Count thinking/reasoning blocks
                        if part.get("type") == "thinking":
                            stats.thinking_blocks += 1
                            continue
                        if part.get("type") == "text":
                            text = part.get("text")
                            if isinstance(text, str) and text.strip():
                                if not text_found:
                                    stats.assistant_messages += 1
                                    text_found = True
                                chat_rows.append(
                                    {
                                        "source": "claude",
                                        "timestamp": timestamp,
                                        "session_id": session_id,
                                        "project_name": stats.project_name,
                                        "role": "assistant",
                                        "text": text.strip(),
                                    }
                                )
                            continue

                        if part.get("type") != "tool_use":
                            continue

                        tool_name = part.get("name") or "unknown"
                        tool_input = part.get("input") or {}
                        query_text = _extract_query_text_from_payload(tool_name, tool_input)
                        target_files = _extract_target_files_from_payload(tool_name, tool_input)

                        stats.tool_calls_total += 1
                        stats.tool_counts[tool_name] += 1

                        is_mcp = _is_mcp_claude_tool(tool_name)
                        if is_mcp:
                            stats.mcp_calls += 1
                            stats.mcp_tools[tool_name] += 1

                        if tool_name in CLAUDE_EDIT_TOOLS and isinstance(tool_input, dict):
                            target_file = (
                                tool_input.get("file_path")
                                or tool_input.get("path")
                                or tool_input.get("target_file")
                            )
                            if isinstance(target_file, str) and target_file.strip():
                                stats.files_touched.add(target_file.strip())

                            if tool_name == "Write":
                                content_text = (
                                    tool_input.get("content")
                                    or tool_input.get("text")
                                    or ""
                                )
                                if isinstance(content_text, str):
                                    stats.code_lines_written += _count_lines(content_text)
                            elif tool_name == "Edit":
                                new_string = tool_input.get("new_string") or ""
                                if isinstance(new_string, str):
                                    stats.code_lines_written += _count_lines(new_string)
                            elif tool_name == "MultiEdit":
                                edits = tool_input.get("edits") or []
                                if isinstance(edits, list):
                                    for edit in edits:
                                        if not isinstance(edit, dict):
                                            continue
                                        new_string = edit.get("new_string") or ""
                                        if isinstance(new_string, str):
                                            stats.code_lines_written += _count_lines(new_string)

                        if target_files:
                            stats.files_touched.update(target_files)

                        call_id = str(part.get("id") or "")

                        tool_rows.append(
                            {
                                "source": "claude",
                                "timestamp": timestamp,
                                "session_id": session_id,
                                "project_name": stats.project_name,
                                "tool_name": tool_name,
                                "is_mcp": is_mcp,
                                "query_text": query_text,
                                "target_file": ", ".join(sorted(target_files)) if target_files else "",
                                "call_id": call_id,
                                "success": None,
                                "error_hint": "",
                            }
                        )
                        if call_id:
                            tool_row_index_by_call[(session_id, call_id)] = len(tool_rows) - 1
        except Exception as exc:  # pragma: no cover - defensive for mixed historical logs
            warnings.append(f"Failed to parse Claude file {file_path}: {exc}")

    return chat_rows, tool_rows, warnings


def parse_psyco_analytics(
    codex_root: Path,
    claude_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    stats_store, chat_rows, codex_tool_rows, warnings = _parse_codex_psyco(codex_root)
    claude_chat_rows, claude_tool_rows, claude_warnings = _parse_claude_psyco(
        claude_root, stats_store
    )
    chat_rows.extend(claude_chat_rows)
    tool_rows = codex_tool_rows + claude_tool_rows
    warnings.extend(claude_warnings)

    session_rows: list[dict[str, Any]] = []
    for stats in stats_store.values():
        session_rows.append(
            {
                "source": stats.source,
                "session_id": stats.session_id,
                "project_path": stats.project_path,
                "project_name": stats.project_name,
                "first_timestamp": stats.first_ts,
                "last_timestamp": stats.last_ts,
                "user_messages": stats.user_messages,
                "assistant_messages": stats.assistant_messages,
                "back_forth_pairs": min(stats.user_messages, stats.assistant_messages),
                "tool_calls_total": stats.tool_calls_total,
                "mcp_calls": stats.mcp_calls,
                "mcp_tools": ", ".join(
                    [name for name, _count in stats.mcp_tools.most_common(20)]
                ),
                "code_lines_written": stats.code_lines_written,
                "files_touched_count": len(stats.files_touched),
                "files_touched": ", ".join(sorted(stats.files_touched)[:40]),
                "subagent_count": stats.subagent_count,
                "thinking_blocks": stats.thinking_blocks,
                "avg_turn_duration_ms": (
                    int(sum(stats.turn_durations_ms) / len(stats.turn_durations_ms))
                    if stats.turn_durations_ms
                    else 0
                ),
                "total_turn_duration_ms": sum(stats.turn_durations_ms),
                "rate_limit_max_pct": round(stats.rate_limit_max_pct, 1),
                "rate_limit_daily_max_pct": round(stats.rate_limit_daily_max_pct, 1),
            }
        )

    session_df = pd.DataFrame(session_rows)
    if not session_df.empty:
        session_df["first_timestamp"] = pd.to_datetime(
            session_df["first_timestamp"], utc=True, errors="coerce"
        )
        session_df["last_timestamp"] = pd.to_datetime(
            session_df["last_timestamp"], utc=True, errors="coerce"
        )
        session_df = session_df.sort_values("last_timestamp", ascending=False)

    chat_df = pd.DataFrame(chat_rows)
    if not chat_df.empty:
        chat_df["timestamp"] = pd.to_datetime(chat_df["timestamp"], utc=True, errors="coerce")
        chat_df = chat_df.dropna(subset=["timestamp"]).sort_values("timestamp")

    tool_df = pd.DataFrame(tool_rows)
    if not tool_df.empty:
        tool_df["timestamp"] = pd.to_datetime(tool_df["timestamp"], utc=True, errors="coerce")
        tool_df = tool_df.dropna(subset=["timestamp"]).sort_values("timestamp")

    if session_df.empty:
        session_df = pd.DataFrame(
            columns=[
                "source",
                "session_id",
                "project_path",
                "project_name",
                "first_timestamp",
                "last_timestamp",
                "user_messages",
                "assistant_messages",
                "back_forth_pairs",
                "tool_calls_total",
                "mcp_calls",
                "mcp_tools",
                "code_lines_written",
                "files_touched_count",
                "files_touched",
            ]
        )
    if chat_df.empty:
        chat_df = pd.DataFrame(
            columns=["source", "timestamp", "session_id", "project_name", "role", "text"]
        )
    if tool_df.empty:
        tool_df = pd.DataFrame(
            columns=[
                "source",
                "timestamp",
                "session_id",
                "project_name",
                "tool_name",
                "is_mcp",
                "query_text",
                "target_file",
                "call_id",
                "success",
                "error_hint",
            ]
        )

    return session_df, chat_df, tool_df, warnings
