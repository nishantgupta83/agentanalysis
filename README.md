# Agent Usage Dashboard

Local Streamlit dashboard for:
- Codex usage from `~/.codex/sessions/*.jsonl`
- Claude Code usage from `~/.claude/projects/**/*.jsonl`

It gives:
- project-level and model-level usage views
- session/event-level granular drill-down
- psyco analysis tab for session back-and-forth, AI code line/file activity, and MCP usage
- tool-call activity
- estimated USD cost breakdown using either token pricing or monthly subscription allocation

## Quick Start

1. Create and activate a virtualenv (optional but recommended):
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run dashboard:
```bash
streamlit run app.py
```

## Pricing Accuracy

You can choose cost mode in the sidebar:

- `Fixed Monthly Subscription`:
  - defaulted to `Codex $20/month` and `Claude $20/month`
  - allocates monthly fee down to events/projects by token weight (or event count)
- `Token Pricing`:
  - uses local token telemetry and `config/pricing.json`

For `Token Pricing` mode:
- update `config/pricing.json` with your exact contracted rates
- any model without an explicit match uses the `default` rates
- wildcards are supported, for example `"claude-haiku-4-5-*"` in `models`

## Parsing Notes

- Codex:
  - Uses `token_count` events in session JSONL.
  - Per-event usage is computed as delta from cumulative `total_token_usage`.
  - Duplicate telemetry events are naturally ignored via zero-delta filtering.
- Claude:
  - Uses assistant `message.usage` from project JSONL.
  - Dedupes repeated streaming entries by `(sessionId, requestId)`.
  - Captures tool calls from `message.content` entries with `type=tool_use`.

## Main Files

- `app.py`: Streamlit UI and visualizations
- `dashboard/parsers.py`: Codex/Claude log parsers
- `dashboard/psyco.py`: session chat/activity/MCP analytics parser
- `dashboard/pricing.py`: pricing resolution + cost computation
- `config/pricing.json`: editable pricing table
