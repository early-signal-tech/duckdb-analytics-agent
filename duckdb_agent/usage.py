"""Token totals and cost estimates."""


# USD per million tokens, for cost estimates in traces (platform.claude.com pricing page,
# checked 2026-11-15). Output includes thinking tokens. 5-minute cache writes cost 1.25x
# the input rate on every model; cache reads are per model ("cache_read" multiplier).
# Haiku 5.5 has two rate cards, picked per request by prompt size (input + cache write +
# cache read tokens): up to 100K tokens, and above. Sonnet and Opus price their whole 1M
# context at one rate. Models not listed get no estimate (None).
# Estimates only: check the pricing page before budgeting on them.
MODEL_PRICING = {
    "claude-haiku-5-5": {
        "cache_read": 0.1,
        "long_prompt_threshold": 100_000,
        "standard": {"input": 0.10, "output": 0.50},
        "long_prompt": {"input": 0.50, "output": 2.50},
    },
    "claude-sonnet-5-5": {
        "cache_read": 0.05,
        "standard": {"input": 2.00, "output": 10.00},
    },
    "claude-opus-5-5": {
        "cache_read": 0.05,
        "standard": {"input": 4.00, "output": 20.00},
    },
}


CACHE_WRITE_MULTIPLIER = 1.25  # 5-minute TTL (CACHE_CONTROL)


def estimate_cost(model: str, usage) -> float | None:
    """Estimated USD cost of one API call from its usage, or None for unpriced models."""
    pricing = MODEL_PRICING.get(model)
    if pricing is None:
        return None
    input_tokens = getattr(usage, "input_tokens", None) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", None) or 0
    cache_read = getattr(usage, "cache_read_input_tokens", None) or 0
    output_tokens = getattr(usage, "output_tokens", None) or 0
    prompt_tokens = input_tokens + cache_write + cache_read
    threshold = pricing.get("long_prompt_threshold")
    rates = pricing["long_prompt" if threshold is not None and prompt_tokens > threshold else "standard"]
    input_rate = rates["input"] / 1_000_000
    return (
        input_tokens * input_rate
        + cache_write * input_rate * CACHE_WRITE_MULTIPLIER
        + cache_read * input_rate * pricing["cache_read"]
        + output_tokens * rates["output"] / 1_000_000
    )


USAGE_FIELDS = ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens")


def add_usage(totals: dict, usage) -> None:
    """Add one response's usage to running totals. Cache fields are None when unused."""
    for name in USAGE_FIELDS:
        totals[name] = totals.get(name, 0) + (getattr(usage, name, None) or 0)
