# API Reference

## `GET /api/dashboard_data.php`

Returns the dashboard payload for a requested time window.

### Query params

- `days`: integer from `1` to `365`

### Response shape

```json
{
  "ok": true,
  "days": 30,
  "range_start": "2026-03-01",
  "range_end": "2026-03-30",
  "metadata": {},
  "source_rows": [],
  "project_rows": [],
  "model_rows": [],
  "session_rows": [],
  "tool_call_rows": []
}
```

### Backing tables

- `usage_dashboard_metadata`
- `usage_sources_daily`
- `usage_projects_daily`
- `usage_models_daily`
- `usage_sessions_daily`
- `usage_tool_calls`

### Notes

- The browser calls this endpoint.
- The endpoint reads from Supabase server-side using the service-role key.
- The service-role key must never be shipped in browser JavaScript.

## `GET /api/views.php`

Returns a simple file-based visitor counter.

### Response shape

```json
{
  "ok": true,
  "count": 42
}
```

### Notes

- This is independent of Supabase.
- It increments a local file counter on real page loads.

