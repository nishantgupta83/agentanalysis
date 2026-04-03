-- Add hero section metadata columns for the public dashboard
ALTER TABLE usage_dashboard_metadata
  ADD COLUMN IF NOT EXISTS ai_leverage_score float8 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS longest_streak_days int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS current_streak_days int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_active_days int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS avg_claude_session_min float8 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS avg_codex_session_min float8 DEFAULT 0;
