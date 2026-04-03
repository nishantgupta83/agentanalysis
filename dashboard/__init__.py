"""Agent dashboard package."""

import json
from pathlib import Path
from typing import Any


def iter_jsonl(path: Path) -> Any:
    """Yield parsed JSON objects from a JSONL file, skipping empty/malformed lines."""
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

