from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

USAGE_COLUMNS = [
    "source",
    "timestamp",
    "session_id",
    "request_id",
    "project_path",
    "project_name",
    "model",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "total_tokens",
    "raw_file",
]

TOOL_COLUMNS = [
    "source",
    "timestamp",
    "session_id",
    "project_path",
    "project_name",
    "model",
    "tool_name",
    "request_id",
    "raw_file",
]


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _iter_jsonl(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


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


def _parse_codex_file(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    usage_rows: list[dict[str, Any]] = []
    tool_rows: list[dict[str, Any]] = []

    session_id = path.stem
    project_path = ""
    current_model = "unknown"
    prev_totals: dict[str, int] | None = None

    for item in _iter_jsonl(path):
        ts = item.get("timestamp")
        event_type = item.get("type")
        payload = item.get("payload") or {}

        if event_type == "session_meta":
            session_id = payload.get("id") or session_id
            project_path = payload.get("cwd") or project_path
            current_model = payload.get("model") or current_model
            continue

        if event_type == "turn_context":
            project_path = payload.get("cwd") or project_path
            current_model = payload.get("model") or current_model
            continue

        if event_type == "response_item" and payload.get("type") == "function_call":
            tool_rows.append(
                {
                    "source": "codex",
                    "timestamp": ts,
                    "session_id": session_id,
                    "project_path": project_path,
                    "project_name": _project_name(project_path, path),
                    "model": current_model,
                    "tool_name": payload.get("name") or "unknown",
                    "request_id": payload.get("call_id"),
                    "raw_file": str(path),
                }
            )
            continue

        if event_type != "event_msg" or payload.get("type") != "token_count":
            continue

        total_usage = (payload.get("info") or {}).get("total_token_usage") or {}
        if not total_usage:
            continue

        current_totals = {
            "input_tokens": _safe_int(total_usage.get("input_tokens")),
            "output_tokens": _safe_int(total_usage.get("output_tokens")),
            "cache_read_tokens": _safe_int(total_usage.get("cached_input_tokens")),
            "reasoning_tokens": _safe_int(total_usage.get("reasoning_output_tokens")),
        }

        if prev_totals is None:
            deltas = current_totals.copy()
        else:
            deltas = {}
            for key, value in current_totals.items():
                prev_value = prev_totals.get(key, 0)
                deltas[key] = value - prev_value if value >= prev_value else value

        prev_totals = current_totals
        if sum(deltas.values()) <= 0:
            continue

        usage_rows.append(
            {
                "source": "codex",
                "timestamp": ts,
                "session_id": session_id,
                "request_id": None,
                "project_path": project_path,
                "project_name": _project_name(project_path, path),
                "model": current_model,
                "input_tokens": deltas["input_tokens"],
                "output_tokens": deltas["output_tokens"],
                "cache_read_tokens": deltas["cache_read_tokens"],
                "cache_write_tokens": 0,
                "reasoning_tokens": deltas["reasoning_tokens"],
                "total_tokens": (
                    deltas["input_tokens"]
                    + deltas["output_tokens"]
                    + deltas["cache_read_tokens"]
                ),
                "raw_file": str(path),
            }
        )

    return usage_rows, tool_rows


def _parse_claude_file(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    usage_rows: list[dict[str, Any]] = []
    tool_rows: list[dict[str, Any]] = []

    seen_usage: set[tuple[str, str]] = set()
    seen_tools: set[tuple[str, str]] = set()

    for item in _iter_jsonl(path):
        if item.get("type") != "assistant":
            continue

        ts = item.get("timestamp")
        session_id = item.get("sessionId") or path.stem
        message = item.get("message") or {}
        usage = message.get("usage") or {}
        request_id = item.get("requestId") or message.get("id") or item.get("uuid")
        project_path = item.get("cwd") or item.get("project") or ""
        model = message.get("model") or "unknown"

        if usage:
            dedupe_key = (session_id, str(request_id))
            if dedupe_key not in seen_usage:
                seen_usage.add(dedupe_key)
                input_tokens = _safe_int(usage.get("input_tokens"))
                output_tokens = _safe_int(usage.get("output_tokens"))
                cache_read_tokens = _safe_int(usage.get("cache_read_input_tokens"))
                cache_write_tokens = _safe_int(usage.get("cache_creation_input_tokens"))
                reasoning_tokens = _safe_int(usage.get("reasoning_output_tokens"))

                usage_rows.append(
                    {
                        "source": "claude",
                        "timestamp": ts,
                        "session_id": session_id,
                        "request_id": request_id,
                        "project_path": project_path,
                        "project_name": _project_name(project_path, path),
                        "model": model,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cache_read_tokens": cache_read_tokens,
                        "cache_write_tokens": cache_write_tokens,
                        "reasoning_tokens": reasoning_tokens,
                        "total_tokens": (
                            input_tokens
                            + output_tokens
                            + cache_read_tokens
                            + cache_write_tokens
                        ),
                        "raw_file": str(path),
                    }
                )

        content = message.get("content") or []
        for content_item in content:
            if not isinstance(content_item, dict):
                continue
            if content_item.get("type") != "tool_use":
                continue
            tool_id = content_item.get("id") or f"{request_id}:{content_item.get('name')}"
            dedupe_key = (session_id, str(tool_id))
            if dedupe_key in seen_tools:
                continue
            seen_tools.add(dedupe_key)
            tool_rows.append(
                {
                    "source": "claude",
                    "timestamp": ts,
                    "session_id": session_id,
                    "project_path": project_path,
                    "project_name": _project_name(project_path, path),
                    "model": model,
                    "tool_name": content_item.get("name") or "unknown",
                    "request_id": request_id,
                    "raw_file": str(path),
                }
            )

    return usage_rows, tool_rows


def parse_usage_logs(
    codex_root: Path,
    claude_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    usage_rows: list[dict[str, Any]] = []
    tool_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    codex_files = sorted(codex_root.rglob("*.jsonl")) if codex_root.exists() else []
    claude_files = sorted(claude_root.rglob("*.jsonl")) if claude_root.exists() else []

    for file_path in codex_files:
        try:
            usage, tools = _parse_codex_file(file_path)
            usage_rows.extend(usage)
            tool_rows.extend(tools)
        except Exception as exc:  # pragma: no cover - defensive for mixed log formats
            warnings.append(f"Failed to parse Codex file {file_path}: {exc}")

    for file_path in claude_files:
        try:
            usage, tools = _parse_claude_file(file_path)
            usage_rows.extend(usage)
            tool_rows.extend(tools)
        except Exception as exc:  # pragma: no cover - defensive for mixed log formats
            warnings.append(f"Failed to parse Claude file {file_path}: {exc}")

    usage_df = pd.DataFrame(usage_rows)
    if usage_df.empty:
        usage_df = pd.DataFrame(columns=USAGE_COLUMNS)
    else:
        usage_df["timestamp"] = pd.to_datetime(
            usage_df["timestamp"], utc=True, errors="coerce"
        )
        for column in [
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "total_tokens",
        ]:
            usage_df[column] = pd.to_numeric(usage_df[column], errors="coerce").fillna(0)
        usage_df = usage_df.dropna(subset=["timestamp"])

    tool_df = pd.DataFrame(tool_rows)
    if tool_df.empty:
        tool_df = pd.DataFrame(columns=TOOL_COLUMNS)
    else:
        tool_df["timestamp"] = pd.to_datetime(
            tool_df["timestamp"], utc=True, errors="coerce"
        )
        tool_df = tool_df.dropna(subset=["timestamp"])

    return usage_df, tool_df, warnings
