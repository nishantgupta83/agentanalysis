#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from supabase import create_client

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard.parsers import parse_usage_logs
from dashboard.psyco import parse_psyco_analytics
from dashboard.rollups import (
    build_daily_rollups,
    build_dashboard_metadata,
    clean_json_value,
    df_to_records,
    prepare_tool_calls,
    prepare_usage_events,
)

TIME_SERIES_TABLES = (
    "usage_events",
    "usage_tool_calls",
    "usage_sessions_daily",
    "usage_projects_daily",
    "usage_models_daily",
    "usage_sources_daily",
)


def _upsert_rows(client: Any, table_name: str, rows: list[dict[str, Any]], on_conflict: str, chunk_size: int = 500) -> int:
    if not rows:
        return 0
    written = 0
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        client.table(table_name).upsert(chunk, on_conflict=on_conflict).execute()
        written += len(chunk)
    return written


def _delete_all(client: Any, table_name: str) -> None:
    if table_name == "usage_dashboard_metadata":
        client.table(table_name).delete().gte("metadata_key", "").execute()
        return
    if table_name == "usage_sync_runs":
        client.table(table_name).delete().gte("started_at_utc", "1900-01-01T00:00:00+00:00").execute()
        return
    client.table(table_name).delete().gte("usage_date", "1900-01-01").execute()


def _delete_from_cutoff(client: Any, table_name: str, cutoff_date: str) -> None:
    client.table(table_name).delete().gte("usage_date", cutoff_date).execute()


def _records_for_write(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [{key: clean_json_value(value) for key, value in row.items()} for row in df.to_dict(orient="records")]


def _filter_by_days_back(df: pd.DataFrame, days_back: int) -> tuple[pd.DataFrame, str]:
    if df.empty or "usage_date" not in df.columns:
        cutoff = datetime.now(timezone.utc).date().isoformat()
        return df.copy(), cutoff
    max_day = df["usage_date"].max()
    cutoff_day = max_day - timedelta(days=max(days_back - 1, 0))
    return df[df["usage_date"] >= cutoff_day].copy(), cutoff_day.isoformat()


def _print_counts(label: str, df: pd.DataFrame) -> None:
    print(f"[sync] {label}={len(df)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse local AI logs and upsert analytics tables into Supabase.")
    parser.add_argument("--codex-root", default=os.getenv("CODEX_ROOT", str(Path.home() / ".codex/sessions")))
    parser.add_argument("--claude-root", default=os.getenv("CLAUDE_ROOT", str(Path.home() / ".claude/projects")))
    parser.add_argument("--supabase-url", default=os.getenv("SUPABASE_URL", ""))
    parser.add_argument("--supabase-service-role-key", default=os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""))
    parser.add_argument("--days-back", type=int, default=int(os.getenv("DAYS_BACK", "90")))
    parser.add_argument("--dry-run", action="store_true", help="Parse and aggregate without writing to Supabase.")
    parser.add_argument("--full-refresh", action="store_true", help="Delete and rebuild all destination tables.")
    args = parser.parse_args()

    codex_root = Path(args.codex_root).expanduser()
    claude_root = Path(args.claude_root).expanduser()
    print(f"[sync] codex_root={codex_root}")
    print(f"[sync] claude_root={claude_root}")

    raw_usage_df, raw_tool_df, usage_warnings = parse_usage_logs(codex_root, claude_root)
    psyco_session_df, _psyco_chat_df, psyco_tool_df, psyco_warnings = parse_psyco_analytics(codex_root, claude_root)
    warnings = usage_warnings + psyco_warnings

    usage_df = prepare_usage_events(raw_usage_df)
    tool_calls_df = prepare_tool_calls(raw_tool_df, psyco_tool_df)
    session_daily_df, project_daily_df, model_daily_df, source_daily_df = build_daily_rollups(
        usage_df,
        tool_calls_df,
        psyco_session_df,
    )
    metadata_row = build_dashboard_metadata(usage_df, tool_calls_df, session_daily_df, psyco_session_df)

    _print_counts("usage_events", usage_df)
    _print_counts("usage_tool_calls", tool_calls_df)
    _print_counts("usage_sessions_daily", session_daily_df)
    _print_counts("usage_projects_daily", project_daily_df)
    _print_counts("usage_models_daily", model_daily_df)
    _print_counts("usage_sources_daily", source_daily_df)
    print(f"[sync] warnings={len(warnings)}")

    usage_filtered, cutoff_iso = _filter_by_days_back(usage_df, args.days_back)
    tool_filtered, _ = _filter_by_days_back(tool_calls_df, args.days_back)
    session_filtered, _ = _filter_by_days_back(session_daily_df, args.days_back)
    project_filtered, _ = _filter_by_days_back(project_daily_df, args.days_back)
    model_filtered, _ = _filter_by_days_back(model_daily_df, args.days_back)
    source_filtered, _ = _filter_by_days_back(source_daily_df, args.days_back)

    if args.dry_run:
        print(f"[sync] cutoff={cutoff_iso}")
        print("[sync] dry-run complete")
        return

    if not args.supabase_url or not args.supabase_service_role_key:
        raise SystemExit(
            "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or pass them as flags."
        )

    client = create_client(args.supabase_url, args.supabase_service_role_key)

    if args.full_refresh:
        for table_name in TIME_SERIES_TABLES:
            _delete_all(client, table_name)
    else:
        for table_name in TIME_SERIES_TABLES:
            _delete_from_cutoff(client, table_name, cutoff_iso)

    _delete_all(client, "usage_dashboard_metadata")

    updated_at_utc = datetime.now(timezone.utc).isoformat()
    uploaded_counts = {
        "usage_events": _upsert_rows(client, "usage_events", _records_for_write(usage_filtered), "usage_key"),
        "usage_tool_calls": _upsert_rows(client, "usage_tool_calls", _records_for_write(tool_filtered), "tool_call_key"),
        "usage_sessions_daily": _upsert_rows(client, "usage_sessions_daily", df_to_records(session_filtered, updated_at_utc), "usage_date,source,session_id"),
        "usage_projects_daily": _upsert_rows(client, "usage_projects_daily", df_to_records(project_filtered, updated_at_utc), "usage_date,source,project_hash"),
        "usage_models_daily": _upsert_rows(client, "usage_models_daily", df_to_records(model_filtered, updated_at_utc), "usage_date,source,model"),
        "usage_sources_daily": _upsert_rows(client, "usage_sources_daily", df_to_records(source_filtered, updated_at_utc), "usage_date,source"),
    }
    client.table("usage_dashboard_metadata").upsert(
        [{key: clean_json_value(value) for key, value in metadata_row.items()}],
        on_conflict="metadata_key",
    ).execute()

    client.table("usage_sync_runs").insert(
        {
            "status": "success",
            "mode": "full" if args.full_refresh else "incremental",
            "codex_root": str(codex_root),
            "claude_root": str(claude_root),
            "event_rows": uploaded_counts["usage_events"],
            "tool_rows": uploaded_counts["usage_tool_calls"],
            "sessions_daily_rows": uploaded_counts["usage_sessions_daily"],
            "projects_daily_rows": uploaded_counts["usage_projects_daily"],
            "models_daily_rows": uploaded_counts["usage_models_daily"],
            "sources_daily_rows": uploaded_counts["usage_sources_daily"],
            "warning_count": len(warnings),
            "warning_preview": "; ".join(warnings[:5])[:500],
            "started_at_utc": updated_at_utc,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    ).execute()

    print(f"[sync] cutoff={cutoff_iso}")
    for table_name, count in uploaded_counts.items():
        print(f"[sync] wrote {count} rows -> {table_name}")
    print("[sync] dashboard metadata upserted")


if __name__ == "__main__":
    main()
