-- Supabase schema for personal agent usage dashboard
-- All tables follow usage_<name> pattern as requested.

create extension if not exists pgcrypto;

create table if not exists usage_events (
    usage_key text primary key,
    source text not null check (source in ('codex', 'claude')),
    timestamp_utc timestamptz not null,
    usage_date date not null,
    session_id text not null,
    request_id text,
    project_name text not null,
    project_hash text not null,
    project_path text,
    model text,
    input_tokens bigint not null default 0,
    output_tokens bigint not null default 0,
    cache_read_tokens bigint not null default 0,
    cache_write_tokens bigint not null default 0,
    reasoning_tokens bigint not null default 0,
    total_tokens bigint not null default 0,
    cost_usd numeric(14, 6) not null default 0,
    raw_file text,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists usage_events_usage_date_idx on usage_events (usage_date);
create index if not exists usage_events_source_usage_date_idx on usage_events (source, usage_date);
create index if not exists usage_events_project_hash_idx on usage_events (project_hash);
create index if not exists usage_events_session_idx on usage_events (source, session_id);

create table if not exists usage_tool_calls (
    tool_call_key text primary key,
    source text not null check (source in ('codex', 'claude')),
    timestamp_utc timestamptz not null,
    usage_date date not null,
    session_id text not null,
    request_id text,
    project_name text not null,
    project_hash text not null,
    model text,
    tool_name text not null,
    is_mcp boolean,
    success boolean,
    query_text text,
    target_file text,
    error_hint text,
    raw_file text,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists usage_tool_calls_usage_date_idx on usage_tool_calls (usage_date);
create index if not exists usage_tool_calls_source_usage_date_idx on usage_tool_calls (source, usage_date);
create index if not exists usage_tool_calls_project_hash_idx on usage_tool_calls (project_hash);
create index if not exists usage_tool_calls_session_idx on usage_tool_calls (source, session_id);
create index if not exists usage_tool_calls_tool_name_idx on usage_tool_calls (tool_name);

create table if not exists usage_sessions_daily (
    usage_date date not null,
    source text not null check (source in ('codex', 'claude')),
    session_id text not null,
    project_name text not null,
    project_hash text not null,
    events bigint not null default 0,
    tool_calls bigint not null default 0,
    mcp_calls bigint not null default 0,
    failed_tool_calls bigint not null default 0,
    user_messages bigint not null default 0,
    assistant_messages bigint not null default 0,
    back_forth_pairs bigint not null default 0,
    code_lines_written bigint not null default 0,
    files_touched_count bigint not null default 0,
    total_tokens bigint not null default 0,
    cost_usd numeric(14, 6) not null default 0,
    model_count integer not null default 0,
    first_timestamp_utc timestamptz,
    last_timestamp_utc timestamptz,
    updated_at timestamptz not null default now(),
    primary key (usage_date, source, session_id)
);

create index if not exists usage_sessions_daily_source_idx on usage_sessions_daily (source, usage_date);
create index if not exists usage_sessions_daily_project_hash_idx on usage_sessions_daily (project_hash);

create table if not exists usage_projects_daily (
    usage_date date not null,
    source text not null check (source in ('codex', 'claude')),
    project_name text not null,
    project_hash text not null,
    sessions bigint not null default 0,
    events bigint not null default 0,
    tool_calls bigint not null default 0,
    mcp_calls bigint not null default 0,
    failed_tool_calls bigint not null default 0,
    user_messages bigint not null default 0,
    assistant_messages bigint not null default 0,
    code_lines_written bigint not null default 0,
    files_touched_count bigint not null default 0,
    total_tokens bigint not null default 0,
    cost_usd numeric(14, 6) not null default 0,
    updated_at timestamptz not null default now(),
    primary key (usage_date, source, project_hash)
);

create index if not exists usage_projects_daily_source_idx on usage_projects_daily (source, usage_date);

create table if not exists usage_models_daily (
    usage_date date not null,
    source text not null check (source in ('codex', 'claude')),
    model text not null,
    events bigint not null default 0,
    sessions bigint not null default 0,
    total_tokens bigint not null default 0,
    cost_usd numeric(14, 6) not null default 0,
    updated_at timestamptz not null default now(),
    primary key (usage_date, source, model)
);

create index if not exists usage_models_daily_source_idx on usage_models_daily (source, usage_date);

create table if not exists usage_sources_daily (
    usage_date date not null,
    source text not null check (source in ('codex', 'claude')),
    projects bigint not null default 0,
    sessions bigint not null default 0,
    events bigint not null default 0,
    tool_calls bigint not null default 0,
    mcp_calls bigint not null default 0,
    failed_tool_calls bigint not null default 0,
    total_tokens bigint not null default 0,
    cost_usd numeric(14, 6) not null default 0,
    active_models integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (usage_date, source)
);

create table if not exists usage_dashboard_metadata (
    metadata_key text primary key,
    generated_at_utc timestamptz not null,
    range_start date,
    range_end date,
    total_events bigint not null default 0,
    total_sessions bigint not null default 0,
    total_projects bigint not null default 0,
    total_tokens bigint not null default 0,
    total_cost_usd numeric(14, 6) not null default 0,
    total_tool_calls bigint not null default 0,
    total_mcp_calls bigint not null default 0,
    total_code_lines_written bigint not null default 0,
    rolling_7d_cost_usd numeric(14, 6) not null default 0,
    rolling_30d_cost_usd numeric(14, 6) not null default 0,
    rolling_7d_tokens bigint not null default 0,
    rolling_30d_tokens bigint not null default 0,
    peak_day date,
    peak_day_cost_usd numeric(14, 6) not null default 0,
    updated_at timestamptz not null default now()
);

create table if not exists usage_sync_runs (
    run_id uuid primary key default gen_random_uuid(),
    started_at_utc timestamptz not null default now(),
    finished_at_utc timestamptz,
    status text not null check (status in ('running', 'success', 'failed')),
    mode text not null default 'incremental' check (mode in ('incremental', 'full')),
    codex_root text,
    claude_root text,
    event_rows bigint not null default 0,
    tool_rows bigint not null default 0,
    sessions_daily_rows bigint not null default 0,
    projects_daily_rows bigint not null default 0,
    models_daily_rows bigint not null default 0,
    sources_daily_rows bigint not null default 0,
    warning_count integer not null default 0,
    warning_preview text,
    error_message text
);

create index if not exists usage_sync_runs_started_at_idx on usage_sync_runs (started_at_utc desc);

-- Public dashboard tables: allow read for anon + authenticated.
alter table usage_sources_daily enable row level security;
alter table usage_projects_daily enable row level security;
alter table usage_models_daily enable row level security;
alter table usage_dashboard_metadata enable row level security;

drop policy if exists usage_sources_daily_read_public on usage_sources_daily;
create policy usage_sources_daily_read_public on usage_sources_daily
for select to anon, authenticated
using (true);

drop policy if exists usage_projects_daily_read_public on usage_projects_daily;
create policy usage_projects_daily_read_public on usage_projects_daily
for select to anon, authenticated
using (true);

drop policy if exists usage_models_daily_read_public on usage_models_daily;
create policy usage_models_daily_read_public on usage_models_daily
for select to anon, authenticated
using (true);

drop policy if exists usage_dashboard_metadata_read_public on usage_dashboard_metadata;
create policy usage_dashboard_metadata_read_public on usage_dashboard_metadata
for select to anon, authenticated
using (true);

-- Keep raw tables private by default.
alter table usage_events enable row level security;
alter table usage_tool_calls enable row level security;
alter table usage_sessions_daily enable row level security;
alter table usage_sync_runs enable row level security;
