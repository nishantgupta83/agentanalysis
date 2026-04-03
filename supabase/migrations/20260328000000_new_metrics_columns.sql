-- Add new metric columns: cache efficiency, reasoning tokens, turn duration,
-- subagent depth, rate limit gauge.

-- usage_sessions_daily: per-session new metrics
DO $$ BEGIN
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS cache_read_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS cache_write_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS reasoning_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS subagent_count bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS thinking_blocks bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS avg_turn_duration_ms bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS total_turn_duration_ms bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS rate_limit_max_pct numeric(5, 1) NOT NULL DEFAULT 0;
  ALTER TABLE usage_sessions_daily ADD COLUMN IF NOT EXISTS rate_limit_daily_max_pct numeric(5, 1) NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- usage_sources_daily: aggregated new metrics (public-read via RLS)
DO $$ BEGIN
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS cache_read_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS cache_write_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS reasoning_tokens bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS subagent_count bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS thinking_blocks bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS avg_turn_duration_ms bigint NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS rate_limit_max_pct numeric(5, 1) NOT NULL DEFAULT 0;
  ALTER TABLE usage_sources_daily ADD COLUMN IF NOT EXISTS rate_limit_daily_max_pct numeric(5, 1) NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
