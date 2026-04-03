from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

USAGE_EVENT_COLUMNS = [
    "usage_key",
    "source",
    "timestamp",
    "usage_date",
    "session_id",
    "request_id",
    "project_name",
    "project_hash",
    "project_path",
    "model",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "total_tokens",
    "cost_usd",
    "raw_file",
]

TOOL_CALL_COLUMNS = [
    "tool_call_key",
    "source",
    "timestamp",
    "usage_date",
    "session_id",
    "request_id",
    "project_name",
    "project_hash",
    "model",
    "tool_name",
    "is_mcp",
    "success",
    "query_text",
    "target_file",
    "error_hint",
    "raw_file",
]


def _normalize_project_name(value: Any) -> str:
    try:
        if pd.isna(value):
            return "unknown"
    except Exception:
        pass
    text = str(value or "").strip()
    return text or "unknown"


def _hash_project_name(project_name: Any) -> str:
    cleaned = _normalize_project_name(project_name).lower()
    return "proj_" + hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:10]


def _sanitize_path(raw_path: Any) -> str:
    text = str(raw_path or "").strip()
    if not text:
        return ""
    try:
        text = str(Path(text).resolve())
    except (OSError, ValueError):
        pass
    home = str(Path.home())
    if text.startswith(home):
        text = "~" + text[len(home):]
    for prefix in ("/Users/", "/home/"):
        if prefix in text:
            _, remainder = text.split(prefix, 1)
            parts = remainder.split("/", 1)
            text = "~/" + parts[1] if len(parts) > 1 else "~"
    return text


def _stable_usage_key(row: pd.Series) -> str:
    raw = "|".join(
        [
            str(row.get("source", "")),
            str(row.get("timestamp", "")),
            str(row.get("session_id", "")),
            str(row.get("request_id", "")),
            str(row.get("project_name", "")),
            str(row.get("model", "")),
            str(row.get("input_tokens", 0)),
            str(row.get("output_tokens", 0)),
            str(row.get("cache_read_tokens", 0)),
            str(row.get("cache_write_tokens", 0)),
            str(row.get("reasoning_tokens", 0)),
            str(row.get("total_tokens", 0)),
            str(row.get("raw_file", "")),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _stable_tool_call_key(row: pd.Series) -> str:
    raw = "|".join(
        [
            str(row.get("source", "")),
            str(row.get("timestamp", "")),
            str(row.get("session_id", "")),
            str(row.get("request_id", "")),
            str(row.get("project_name", "")),
            str(row.get("tool_name", "")),
            str(row.get("model", "")),
            str(row.get("query_text", "")),
            str(row.get("target_file", "")),
            str(row.get("raw_file", "")),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _coerce_numeric(df: pd.DataFrame, columns: list[str], float_columns: set[str] | None = None) -> pd.DataFrame:
    float_columns = float_columns or set()
    for column in columns:
        if column not in df.columns:
            df[column] = 0.0 if column in float_columns else 0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0 if column in float_columns else 0)
    return df


def prepare_usage_events(raw_usage_df: pd.DataFrame) -> pd.DataFrame:
    if raw_usage_df.empty:
        return pd.DataFrame(columns=USAGE_EVENT_COLUMNS)

    df = raw_usage_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    required = [
        "source",
        "session_id",
        "request_id",
        "project_name",
        "project_path",
        "model",
        "raw_file",
    ]
    for column in required:
        if column not in df.columns:
            df[column] = None

    df = _coerce_numeric(
        df,
        [
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "total_tokens",
            "cost_usd",
        ],
        float_columns={"cost_usd"},
    )

    df["project_name"] = df["project_name"].map(_normalize_project_name)
    df["project_hash"] = df["project_name"].map(_hash_project_name)
    df["usage_date"] = df["timestamp"].dt.date
    df["project_path"] = df["project_path"].map(_sanitize_path)
    df["raw_file"] = df["raw_file"].map(_sanitize_path)
    df["usage_key"] = df.apply(_stable_usage_key, axis=1)

    return df[USAGE_EVENT_COLUMNS].copy()


def prepare_tool_calls(raw_tool_df: pd.DataFrame, psyco_tool_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["source", "timestamp", "session_id", "project_name", "tool_name"]

    base_cols = [
        "source",
        "timestamp",
        "session_id",
        "project_name",
        "tool_name",
        "model",
        "request_id",
        "raw_file",
    ]
    base = pd.DataFrame(columns=base_cols)
    if not raw_tool_df.empty:
        base = raw_tool_df.copy()
        base["timestamp"] = pd.to_datetime(base["timestamp"], utc=True, errors="coerce")
        base = base.dropna(subset=["timestamp"])
        for column in base_cols:
            if column not in base.columns:
                base[column] = None
        base["project_name"] = base["project_name"].map(_normalize_project_name)
        base = base[base_cols].sort_values(key_cols)
        base["call_seq"] = base.groupby(key_cols).cumcount()

    enriched_cols = [
        "source",
        "timestamp",
        "session_id",
        "project_name",
        "tool_name",
        "is_mcp",
        "success",
        "query_text",
        "target_file",
        "error_hint",
        "call_id",
    ]
    enriched = pd.DataFrame(columns=enriched_cols)
    if not psyco_tool_df.empty:
        enriched = psyco_tool_df.copy()
        enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True, errors="coerce")
        enriched = enriched.dropna(subset=["timestamp"])
        for column in enriched_cols:
            if column not in enriched.columns:
                enriched[column] = None
        enriched["project_name"] = enriched["project_name"].map(_normalize_project_name)
        enriched = enriched[enriched_cols].sort_values(key_cols)
        enriched["call_seq"] = enriched.groupby(key_cols).cumcount()

    if base.empty and enriched.empty:
        return pd.DataFrame(columns=TOOL_CALL_COLUMNS)

    merged = base.merge(
        enriched,
        on=key_cols + ["call_seq"],
        how="outer",
        suffixes=("_base", "_psyco"),
    )

    for column in ("model", "request_id", "raw_file"):
        if column not in merged.columns:
            merged[column] = None
    for column in ("is_mcp", "success", "query_text", "target_file", "error_hint"):
        if column not in merged.columns:
            merged[column] = None

    merged["project_name"] = merged["project_name"].map(_normalize_project_name)
    merged["project_hash"] = merged["project_name"].map(_hash_project_name)
    merged["usage_date"] = merged["timestamp"].dt.date
    merged["raw_file"] = merged["raw_file"].map(_sanitize_path)
    merged["target_file"] = merged["target_file"].map(_sanitize_path)
    merged["query_text"] = (
        merged["query_text"].fillna("").astype(str).str.strip().str.slice(0, 120).replace("", None)
    )
    merged["tool_call_key"] = merged.apply(_stable_tool_call_key, axis=1)

    result = merged[TOOL_CALL_COLUMNS].sort_values("timestamp").copy()
    return result


def build_daily_rollups(
    usage_df: pd.DataFrame,
    tool_calls_df: pd.DataFrame,
    psyco_session_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    key_cols = ["usage_date", "source", "session_id"]

    usage_session = pd.DataFrame(columns=key_cols)
    if not usage_df.empty:
        usage_session = (
            usage_df.groupby(key_cols, as_index=False)
            .agg(
                events=("usage_key", "count"),
                total_tokens=("total_tokens", "sum"),
                cost_usd=("cost_usd", "sum"),
                model_count=("model", "nunique"),
                first_timestamp_utc=("timestamp", "min"),
                last_timestamp_utc=("timestamp", "max"),
                cache_read_tokens=("cache_read_tokens", "sum"),
                cache_write_tokens=("cache_write_tokens", "sum"),
                reasoning_tokens=("reasoning_tokens", "sum"),
            )
        )
        latest = usage_df.sort_values("timestamp").groupby(key_cols).tail(1)
        latest = latest[key_cols + ["project_name", "project_hash"]]
        usage_session = usage_session.merge(latest, on=key_cols, how="left")

    tool_session = pd.DataFrame(columns=key_cols)
    if not tool_calls_df.empty:
        tool_session = (
            tool_calls_df.groupby(key_cols, as_index=False)
            .agg(
                tool_calls=("tool_call_key", "count"),
                mcp_calls=("is_mcp", lambda series: int(series.astype("boolean").fillna(False).sum())),
                failed_tool_calls=("success", lambda series: int((series.astype("boolean").fillna(True) == False).sum())),
            )
        )

    psyco_session_day = pd.DataFrame(columns=key_cols)
    if not psyco_session_df.empty:
        psyco = psyco_session_df.copy()
        for column in (
            "project_path",
            "project_name",
            "first_timestamp",
            "last_timestamp",
            "user_messages",
            "assistant_messages",
            "back_forth_pairs",
            "code_lines_written",
            "files_touched_count",
            "subagent_count",
            "thinking_blocks",
            "avg_turn_duration_ms",
            "total_turn_duration_ms",
            "rate_limit_max_pct",
            "rate_limit_daily_max_pct",
        ):
            if column not in psyco.columns:
                psyco[column] = 0 if column not in {"project_name", "project_path", "first_timestamp", "last_timestamp"} else None
        psyco["first_timestamp"] = pd.to_datetime(psyco["first_timestamp"], utc=True, errors="coerce")
        psyco["last_timestamp"] = pd.to_datetime(psyco["last_timestamp"], utc=True, errors="coerce")
        psyco["anchor_timestamp"] = psyco["last_timestamp"].combine_first(psyco["first_timestamp"])
        psyco = psyco.dropna(subset=["anchor_timestamp"])
        psyco["usage_date"] = psyco["anchor_timestamp"].dt.date
        psyco["project_name"] = psyco["project_name"].map(_normalize_project_name)
        psyco["project_hash"] = psyco["project_name"].map(_hash_project_name)
        psyco = _coerce_numeric(
            psyco,
            [
                "user_messages",
                "assistant_messages",
                "back_forth_pairs",
                "code_lines_written",
                "files_touched_count",
                "subagent_count",
                "thinking_blocks",
                "avg_turn_duration_ms",
                "total_turn_duration_ms",
                "rate_limit_max_pct",
                "rate_limit_daily_max_pct",
            ],
            float_columns={"rate_limit_max_pct", "rate_limit_daily_max_pct"},
        )
        psyco_session_day = (
            psyco.groupby(key_cols, as_index=False)
            .agg(
                project_name=("project_name", "last"),
                project_hash=("project_hash", "last"),
                user_messages=("user_messages", "sum"),
                assistant_messages=("assistant_messages", "sum"),
                back_forth_pairs=("back_forth_pairs", "sum"),
                code_lines_written=("code_lines_written", "sum"),
                files_touched_count=("files_touched_count", "sum"),
                subagent_count=("subagent_count", "sum"),
                thinking_blocks=("thinking_blocks", "sum"),
                avg_turn_duration_ms=("avg_turn_duration_ms", "mean"),
                total_turn_duration_ms=("total_turn_duration_ms", "sum"),
                rate_limit_max_pct=("rate_limit_max_pct", "max"),
                rate_limit_daily_max_pct=("rate_limit_daily_max_pct", "max"),
            )
        )

    key_frames = [frame[key_cols] for frame in (usage_session, tool_session, psyco_session_day) if not frame.empty]
    if not key_frames:
        empty = pd.DataFrame(columns=["usage_date", "source"])
        return empty.copy(), empty.copy(), empty.copy(), empty.copy()

    session_daily = pd.concat(key_frames, ignore_index=True).drop_duplicates().sort_values(key_cols)
    session_daily = session_daily.merge(usage_session, on=key_cols, how="left")
    session_daily = session_daily.merge(tool_session, on=key_cols, how="left")
    session_daily = session_daily.merge(
        psyco_session_day,
        on=key_cols,
        how="left",
        suffixes=("", "_psyco"),
    )

    if "project_name" not in session_daily.columns:
        session_daily["project_name"] = None
    if "project_hash" not in session_daily.columns:
        session_daily["project_hash"] = None
    if "project_name_psyco" in session_daily.columns:
        session_daily["project_name"] = session_daily["project_name"].combine_first(
            session_daily["project_name_psyco"]
        )
    if "project_hash_psyco" in session_daily.columns:
        session_daily["project_hash"] = session_daily["project_hash"].combine_first(
            session_daily["project_hash_psyco"]
        )
    session_daily = session_daily.drop(
        columns=[column for column in ("project_name_psyco", "project_hash_psyco") if column in session_daily.columns]
    )
    session_daily["project_name"] = session_daily["project_name"].map(_normalize_project_name)
    session_daily["project_hash"] = session_daily["project_hash"].fillna(
        session_daily["project_name"].map(_hash_project_name)
    )

    session_daily = _coerce_numeric(
        session_daily,
        [
            "events",
            "tool_calls",
            "mcp_calls",
            "failed_tool_calls",
            "user_messages",
            "assistant_messages",
            "back_forth_pairs",
            "code_lines_written",
            "files_touched_count",
            "total_tokens",
            "model_count",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "subagent_count",
            "thinking_blocks",
            "avg_turn_duration_ms",
            "total_turn_duration_ms",
            "rate_limit_max_pct",
            "rate_limit_daily_max_pct",
            "cost_usd",
        ],
        float_columns={"cost_usd", "rate_limit_max_pct", "rate_limit_daily_max_pct"},
    )

    session_daily["avg_turn_duration_ms"] = session_daily["avg_turn_duration_ms"].round().astype(int)
    session_daily["total_turn_duration_ms"] = session_daily["total_turn_duration_ms"].round().astype(int)
    session_daily["rate_limit_max_pct"] = session_daily["rate_limit_max_pct"].round(1)
    session_daily["rate_limit_daily_max_pct"] = session_daily["rate_limit_daily_max_pct"].round(1)

    project_daily = (
        session_daily.groupby(["usage_date", "source", "project_name", "project_hash"], as_index=False)
        .agg(
            sessions=("session_id", "nunique"),
            events=("events", "sum"),
            tool_calls=("tool_calls", "sum"),
            mcp_calls=("mcp_calls", "sum"),
            failed_tool_calls=("failed_tool_calls", "sum"),
            user_messages=("user_messages", "sum"),
            assistant_messages=("assistant_messages", "sum"),
            code_lines_written=("code_lines_written", "sum"),
            files_touched_count=("files_touched_count", "sum"),
            total_tokens=("total_tokens", "sum"),
            cost_usd=("cost_usd", "sum"),
        )
    )

    model_daily = pd.DataFrame(
        columns=["usage_date", "source", "model", "events", "sessions", "total_tokens", "cost_usd"]
    )
    if not usage_df.empty:
        model_daily = (
            usage_df.groupby(["usage_date", "source", "model"], as_index=False)
            .agg(
                events=("usage_key", "count"),
                sessions=("session_id", "nunique"),
                total_tokens=("total_tokens", "sum"),
                cost_usd=("cost_usd", "sum"),
            )
        )

    source_daily = (
        session_daily.groupby(["usage_date", "source"], as_index=False)
        .agg(
            projects=("project_hash", "nunique"),
            sessions=("session_id", "nunique"),
            events=("events", "sum"),
            tool_calls=("tool_calls", "sum"),
            mcp_calls=("mcp_calls", "sum"),
            failed_tool_calls=("failed_tool_calls", "sum"),
            total_tokens=("total_tokens", "sum"),
            cost_usd=("cost_usd", "sum"),
            cache_read_tokens=("cache_read_tokens", "sum"),
            cache_write_tokens=("cache_write_tokens", "sum"),
            reasoning_tokens=("reasoning_tokens", "sum"),
            subagent_count=("subagent_count", "sum"),
            thinking_blocks=("thinking_blocks", "sum"),
            avg_turn_duration_ms=("avg_turn_duration_ms", "mean"),
            rate_limit_max_pct=("rate_limit_max_pct", "max"),
            rate_limit_daily_max_pct=("rate_limit_daily_max_pct", "max"),
        )
    )

    active_models = pd.DataFrame(columns=["usage_date", "source", "active_models"])
    if not model_daily.empty:
        active_models = (
            model_daily.groupby(["usage_date", "source"], as_index=False)
            .agg(active_models=("model", "nunique"))
        )
        source_daily = source_daily.merge(active_models, on=["usage_date", "source"], how="left")
    if "active_models" not in source_daily.columns:
        source_daily["active_models"] = 0
    source_daily["active_models"] = pd.to_numeric(source_daily["active_models"], errors="coerce").fillna(0).astype(int)
    source_daily["avg_turn_duration_ms"] = source_daily["avg_turn_duration_ms"].fillna(0).round().astype(int)
    source_daily["rate_limit_max_pct"] = source_daily["rate_limit_max_pct"].fillna(0).round(1)
    source_daily["rate_limit_daily_max_pct"] = source_daily["rate_limit_daily_max_pct"].fillna(0).round(1)

    return session_daily, project_daily, model_daily, source_daily


def _compute_streaks(unique_dates: list[date]) -> tuple[int, int, int]:
    total_active_days = len(unique_dates)
    if not unique_dates:
        return 0, 0, 0

    longest_streak = 1
    current_streak = 1
    streak = 1
    for index in range(1, len(unique_dates)):
        if (unique_dates[index] - unique_dates[index - 1]).days == 1:
            streak += 1
        else:
            streak = 1
        longest_streak = max(longest_streak, streak)

    today = date.today()
    if (today - unique_dates[-1]).days > 1:
        current_streak = 0
    else:
        current_streak = 1
        for index in range(len(unique_dates) - 2, -1, -1):
            if (unique_dates[index + 1] - unique_dates[index]).days == 1:
                current_streak += 1
            else:
                break

    return total_active_days, longest_streak, current_streak


def build_dashboard_metadata(
    usage_df: pd.DataFrame,
    tool_calls_df: pd.DataFrame,
    session_daily_df: pd.DataFrame,
    psyco_session_df: pd.DataFrame,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    if usage_df.empty:
        return {
            "metadata_key": "global",
            "generated_at_utc": now,
            "range_start": None,
            "range_end": None,
            "total_events": 0,
            "total_sessions": 0,
            "total_projects": 0,
            "total_tokens": 0,
            "total_cost_usd": 0.0,
            "total_tool_calls": 0,
            "total_mcp_calls": 0,
            "total_code_lines_written": 0,
            "rolling_7d_cost_usd": 0.0,
            "rolling_30d_cost_usd": 0.0,
            "rolling_7d_tokens": 0,
            "rolling_30d_tokens": 0,
            "peak_day": None,
            "peak_day_cost_usd": 0.0,
            "ai_leverage_score": 0.0,
            "longest_streak_days": 0,
            "current_streak_days": 0,
            "total_active_days": 0,
            "avg_claude_session_min": 0.0,
            "avg_codex_session_min": 0.0,
            "updated_at": now,
        }

    day_rollup = (
        usage_df.groupby("usage_date", as_index=False)
        .agg(cost_usd=("cost_usd", "sum"), total_tokens=("total_tokens", "sum"))
        .sort_values("usage_date")
    )
    max_day = day_rollup["usage_date"].max()
    min_day = day_rollup["usage_date"].min()
    peak_row = day_rollup.sort_values("cost_usd", ascending=False).iloc[0]
    last_7 = max_day - timedelta(days=6)
    last_30 = max_day - timedelta(days=29)

    total_active_days, longest_streak, current_streak = _compute_streaks(
        sorted(day_rollup["usage_date"].tolist())
    )

    total_user_messages = 0
    if "user_messages" in session_daily_df.columns:
        total_user_messages = int(pd.to_numeric(session_daily_df["user_messages"], errors="coerce").fillna(0).sum())
    ai_leverage_score = round(len(tool_calls_df) / max(total_user_messages, 1), 1)

    avg_claude_session_min = 0.0
    avg_codex_session_min = 0.0
    if not psyco_session_df.empty:
        session_durations = psyco_session_df.copy()
        for column in ("first_timestamp", "last_timestamp", "source"):
            if column not in session_durations.columns:
                session_durations[column] = None
        session_durations["first_timestamp"] = pd.to_datetime(session_durations["first_timestamp"], utc=True, errors="coerce")
        session_durations["last_timestamp"] = pd.to_datetime(session_durations["last_timestamp"], utc=True, errors="coerce")
        session_durations["duration_min"] = (
            (session_durations["last_timestamp"] - session_durations["first_timestamp"]).dt.total_seconds().fillna(0) / 60.0
        )
        for source in ("claude", "codex"):
            subset = session_durations[session_durations["source"] == source]
            if subset.empty:
                continue
            average = round(float(subset["duration_min"].mean()), 1)
            if source == "claude":
                avg_claude_session_min = average
            else:
                avg_codex_session_min = average

    return {
        "metadata_key": "global",
        "generated_at_utc": now,
        "range_start": str(min_day),
        "range_end": str(max_day),
        "total_events": int(len(usage_df)),
        "total_sessions": int(session_daily_df[["source", "session_id"]].drop_duplicates().shape[0]),
        "total_projects": int(usage_df["project_hash"].nunique()),
        "total_tokens": int(pd.to_numeric(usage_df["total_tokens"], errors="coerce").fillna(0).sum()),
        "total_cost_usd": float(pd.to_numeric(usage_df["cost_usd"], errors="coerce").fillna(0).sum()),
        "total_tool_calls": int(len(tool_calls_df)),
        "total_mcp_calls": int(tool_calls_df.get("is_mcp", pd.Series(dtype="boolean")).astype("boolean").fillna(False).sum()) if not tool_calls_df.empty else 0,
        "total_code_lines_written": int(pd.to_numeric(session_daily_df.get("code_lines_written", 0), errors="coerce").fillna(0).sum()),
        "rolling_7d_cost_usd": float(day_rollup[day_rollup["usage_date"] >= last_7]["cost_usd"].sum()),
        "rolling_30d_cost_usd": float(day_rollup[day_rollup["usage_date"] >= last_30]["cost_usd"].sum()),
        "rolling_7d_tokens": int(day_rollup[day_rollup["usage_date"] >= last_7]["total_tokens"].sum()),
        "rolling_30d_tokens": int(day_rollup[day_rollup["usage_date"] >= last_30]["total_tokens"].sum()),
        "peak_day": str(peak_row["usage_date"]),
        "peak_day_cost_usd": float(peak_row["cost_usd"]),
        "ai_leverage_score": ai_leverage_score,
        "longest_streak_days": longest_streak,
        "current_streak_days": current_streak,
        "total_active_days": total_active_days,
        "avg_claude_session_min": avg_claude_session_min,
        "avg_codex_session_min": avg_codex_session_min,
        "updated_at": now,
    }


def clean_json_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def df_to_records(df: pd.DataFrame, updated_at_utc: str) -> list[dict[str, Any]]:
    if df.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        cleaned = {key: clean_json_value(value) for key, value in row.items()}
        cleaned["updated_at"] = updated_at_utc
        rows.append(cleaned)
    return rows
