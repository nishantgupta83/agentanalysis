-- Revoke public read access on sensitive tables.
-- These contain session-level detail (query_text, target_file, error_hint)
-- that should only be accessed server-side via the PHP proxy with service role key.

drop policy if exists usage_sessions_daily_read_public on usage_sessions_daily;
drop policy if exists usage_tool_calls_read_public on usage_tool_calls;
