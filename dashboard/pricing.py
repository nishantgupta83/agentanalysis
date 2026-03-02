from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

PRICE_KEYS = [
    "input_per_million",
    "output_per_million",
    "cache_read_per_million",
    "cache_write_per_million",
]

DEFAULT_PRICING = {
    "default": {
        "input_per_million": 3.0,
        "output_per_million": 15.0,
        "cache_read_per_million": 0.3,
        "cache_write_per_million": 3.0,
    },
    "models": {},
    "aliases": {},
}


def load_pricing(path: Path) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_PRICING
    try:
        pricing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return DEFAULT_PRICING
    if not isinstance(pricing, dict):
        return DEFAULT_PRICING
    pricing.setdefault("default", DEFAULT_PRICING["default"])
    pricing.setdefault("models", {})
    pricing.setdefault("aliases", {})
    return pricing


def _normalize_price_record(record: dict[str, Any], default_record: dict[str, Any]) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for key in PRICE_KEYS:
        value = record.get(key, default_record.get(key, 0.0))
        try:
            normalized[key] = float(value)
        except (TypeError, ValueError):
            normalized[key] = 0.0
    return normalized


def _resolve_price(model: str, pricing: dict[str, Any]) -> tuple[dict[str, float], str]:
    aliases = pricing.get("aliases", {})
    models = pricing.get("models", {})
    default_record = _normalize_price_record(pricing.get("default", {}), {})

    canonical = aliases.get(model, model)

    if canonical in models:
        return _normalize_price_record(models[canonical], default_record), f"model:{canonical}"

    # Prefix wildcard support: example "claude-sonnet-*"
    for key, value in models.items():
        if key.endswith("*") and canonical.startswith(key[:-1]):
            return _normalize_price_record(value, default_record), f"wildcard:{key}"

    return default_record, "default"


def apply_pricing(usage_df: pd.DataFrame, pricing: dict[str, Any]) -> pd.DataFrame:
    if usage_df.empty:
        df = usage_df.copy()
        df["input_cost_usd"] = []
        df["output_cost_usd"] = []
        df["cache_read_cost_usd"] = []
        df["cache_write_cost_usd"] = []
        df["cost_usd"] = []
        df["price_source"] = []
        return df

    df = usage_df.copy()

    resolved = df["model"].fillna("unknown").astype(str).map(lambda m: _resolve_price(m, pricing))
    price_records = [item[0] for item in resolved]
    price_sources = [item[1] for item in resolved]

    price_df = pd.DataFrame(price_records, index=df.index)
    df["price_source"] = price_sources

    df["input_cost_usd"] = (
        df["input_tokens"].astype(float) * price_df["input_per_million"] / 1_000_000.0
    )
    df["output_cost_usd"] = (
        df["output_tokens"].astype(float) * price_df["output_per_million"] / 1_000_000.0
    )
    df["cache_read_cost_usd"] = (
        df["cache_read_tokens"].astype(float)
        * price_df["cache_read_per_million"]
        / 1_000_000.0
    )
    df["cache_write_cost_usd"] = (
        df["cache_write_tokens"].astype(float)
        * price_df["cache_write_per_million"]
        / 1_000_000.0
    )
    df["cost_usd"] = (
        df["input_cost_usd"]
        + df["output_cost_usd"]
        + df["cache_read_cost_usd"]
        + df["cache_write_cost_usd"]
    )

    return df

