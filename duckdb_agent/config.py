"""Run settings: AgentConfig and the defaults behind it."""

import hashlib
from dataclasses import asdict, dataclass

from .serialization import json_dumps


# Result-size caps for what is sent back to Claude. Every tool result stays in the
# conversation and is resent on every later iteration, so large results compound.
MAX_RESULT_ROWS = 200


MAX_RESULT_CHARS = 20_000  # serialized JSON, roughly 6-9k tokens on Haiku 5.5's tokenizer


MAX_CELL_CHARS = 500  # long text values are clipped individually


# Queries running longer than this are cancelled and reported to Claude as an error.
QUERY_TIMEOUT_SECONDS = 30


# Claude Haiku 5.5 ($0.10 / $0.50 per MTok input / output for prompts up to 100K
# tokens). The agent relies on Haiku 5.5-era features (adaptive thinking with summaries,
# effort, mid-conversation system messages), so older models such as Haiku 4.5 won't work.
DEFAULT_MODEL = "claude-haiku-5-5"


# How much the model thinks: low | medium | high | xhigh | max. "medium" is Haiku 5.5's
# default, set explicitly so it's visible.
DEFAULT_EFFORT = "medium"


# Adaptive thinking (the model decides when and how much to think) with summarized
# thinking text, which is logged to agent_traces for debugging. Without "summarized",
# thinking blocks come back with empty text. Thinking blocks are passed back unchanged
# with tool results, because the full response.content is appended to history.
THINKING = {"type": "adaptive", "display": "summarized"}


# Per-response output cap, including thinking tokens. It's a ceiling, not a target: you
# pay only for tokens generated. Too low truncates answers (stop_reason "max_tokens"). The SDK refuses
# non-streaming requests above ~21k, so going higher means switching to streaming.
DEFAULT_MAX_TOKENS = 16_000


# The largest max_tokens a non-streaming call can ask for without the SDK refusing it.
# Not enforced here (a streaming caller could go higher); front ends use it as the limit.
MAX_NON_STREAMING_TOKENS = 21_000


# Prompt caching, 5-minute TTL: loop turns are seconds apart, so every call refreshes
# the entry and the pricier 1-hour TTL would buy nothing. Two breakpoints:
#   1. on the system prompt -- caches tools + system (they render first), which are
#      identical on every call of every run, so even a run's first call can hit it
#      (if another run with the same model and settings ran in the last 5 minutes);
#   2. top-level (automatic) -- moves to the end of the conversation on each call,
#      so each iteration reads everything before it and writes only the new turn.
# Prefixes below the model's minimum (512 tokens on Haiku 5.5) silently don't cache;
# usage shows cache_creation/cache_read of 0.
CACHE_CONTROL = {"type": "ephemeral"}


# Server-side context editing: once a prompt passes the trigger, the API replaces old
# tool results with a placeholder before the model sees them. It must be server-side:
# editing history ourselves would invalidate Haiku 5.5's later thinking blocks (a 400),
# while server-side edits don't count, because the check runs on what we send.
#
# Why 80K: Haiku 5.5 input costs 5x more ($0.50 vs $0.10 per MTok) once a prompt is
# over 100K tokens, and the next turn's results add more on top of the trigger point.
# Each clear rewrites the cached conversation, so clears should be rare and large:
# clear_at_least skips a clear that wouldn't free enough to pay for that cache write.
# The SQL (tool inputs) stays visible, so Claude knows what it ran and can re-run it.
# Schema lookups are never cleared: they're small and stay relevant all run.
#
# These are the defaults; each is an AgentConfig field (context_*), so a run can tune
# them. In DH-1 they're the context compressor's knobs: threshold, keep, clear at least,
# and the sidechain (tools that are never cleared).
CONTEXT_MANAGEMENT_BETA = "context-management-2025-06-27"


CONTEXT_TRIGGER_TOKENS = 80_000


CONTEXT_KEEP_TOOL_USES = 8


CONTEXT_CLEAR_AT_LEAST_TOKENS = 20_000


CONTEXT_PROTECTED_TOOLS = ("list_tables", "describe_table")


# Every tool the agent can call, for checking context_protected_tools (tools.py defines
# them; listed here so config doesn't import tools).
TOOL_NAMES = ("list_tables", "describe_table", "sample_rows", "query_database", "submit_answer")


def build_context_management(
    trigger_tokens: int, keep_tool_uses: int, clear_at_least_tokens: int, protected_tools
) -> dict:
    """The context_management request parameter for these settings."""
    return {
        "edits": [
            {
                "type": "clear_tool_uses_20250919",
                "trigger": {"type": "input_tokens", "value": trigger_tokens},
                "keep": {"type": "tool_uses", "value": keep_tool_uses},
                "clear_at_least": {"type": "input_tokens", "value": clear_at_least_tokens},
                "exclude_tools": list(protected_tools),
            }
        ]
    }


# With the defaults (what AgentConfig().context_management() returns).
CONTEXT_MANAGEMENT = build_context_management(
    CONTEXT_TRIGGER_TOKENS, CONTEXT_KEEP_TOOL_USES, CONTEXT_CLEAR_AT_LEAST_TOKENS, CONTEXT_PROTECTED_TOOLS
)


# SDK-level retries (exponential backoff, honors retry-after) for 408/409/429/5xx
# and connection errors. The SDK default is 2; agent runs are worth waiting for.
API_MAX_RETRIES = 4


# Tool-use rounds before the forced wrap-up call (see MAX_ITERATIONS_WRAP_UP_PROMPT).
DEFAULT_MAX_ITERATIONS = 25


EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")


@dataclass(frozen=True)
class AgentConfig:
    """Every setting that shapes a run, in one place. Defaults are the constants above.

    Frozen, so one config can be shared across runs safely, and recorded with each run
    in agent_runs.config. Change a field with dataclasses.replace(config, effort="high").

    The tool descriptions and system prompt state the result limits and timeout, so
    they're rebuilt from these values. With defaults they're byte-identical to TOOLS
    and SYSTEM_PROMPT, which keeps them cacheable across runs; any change makes a new
    cache entry.
    """
    model: str = DEFAULT_MODEL
    effort: str = DEFAULT_EFFORT
    max_tokens: int = DEFAULT_MAX_TOKENS
    max_iterations: int = DEFAULT_MAX_ITERATIONS
    # Limits on what each query returns to Claude, and how long it may run.
    max_result_rows: int = MAX_RESULT_ROWS
    max_result_chars: int = MAX_RESULT_CHARS
    max_cell_chars: int = MAX_CELL_CHARS
    query_timeout_seconds: float = QUERY_TIMEOUT_SECONDS
    # Offer submit_answer; its input becomes AgentResult.answer.
    structured_answer: bool = False
    # Off switches for API features some gateways reject.
    prompt_caching: bool = True
    context_editing: bool = True
    # Context editing (see CONTEXT_MANAGEMENT): clear old tool results once a prompt
    # passes context_trigger_tokens, keeping the last context_keep_tool_uses, and only
    # if at least context_clear_at_least_tokens would be freed. Results of
    # context_protected_tools are never cleared. Ignored when context_editing is off.
    context_trigger_tokens: int = CONTEXT_TRIGGER_TOKENS
    context_keep_tool_uses: int = CONTEXT_KEEP_TOOL_USES
    context_clear_at_least_tokens: int = CONTEXT_CLEAR_AT_LEAST_TOKENS
    context_protected_tools: tuple[str, ...] = CONTEXT_PROTECTED_TOOLS
    # None builds the default prompt from these settings (build_system_prompt).
    system_prompt: str | None = None

    def __post_init__(self):
        if self.effort not in EFFORT_LEVELS:
            raise ValueError(f"effort must be one of {', '.join(EFFORT_LEVELS)}, not {self.effort!r}")
        for name in ("max_tokens", "max_iterations", "max_result_rows", "max_result_chars", "max_cell_chars"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer, not {value!r}")
        if not self.query_timeout_seconds or self.query_timeout_seconds <= 0:
            raise ValueError(f"query_timeout_seconds must be positive, not {self.query_timeout_seconds!r}")
        if not isinstance(self.context_trigger_tokens, int) or self.context_trigger_tokens < 1:
            raise ValueError(f"context_trigger_tokens must be a positive integer, not {self.context_trigger_tokens!r}")
        for name in ("context_keep_tool_uses", "context_clear_at_least_tokens"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a whole number, 0 or more, not {value!r}")
        # Lists (from JSON) become tuples, so the config stays hashable and frozen.
        protected = self.context_protected_tools
        if isinstance(protected, str) or not all(isinstance(t, str) for t in protected):
            raise ValueError("context_protected_tools must be a list of tool names")
        unknown = sorted(set(protected) - set(TOOL_NAMES))
        if unknown:
            raise ValueError(f"context_protected_tools has unknown tools: {', '.join(unknown)} "
                             f"(tools: {', '.join(TOOL_NAMES)})")
        object.__setattr__(self, "context_protected_tools", tuple(dict.fromkeys(protected)))

    def context_management(self) -> dict:
        """The context_management request parameter for this config."""
        return build_context_management(
            self.context_trigger_tokens, self.context_keep_tool_uses,
            self.context_clear_at_least_tokens, self.context_protected_tools
        )

    def to_dict(self) -> dict:
        """Settings for agent_runs.config. The system prompt actually sent (custom or
        built) is recorded as a short hash, so runs can be grouped by prompt version
        without storing the text each time."""
        # Imported here because prompts imports this module (for AgentConfig).
        from .prompts import build_system_prompt

        settings = asdict(self)
        settings["context_protected_tools"] = list(self.context_protected_tools)  # as JSON has it
        prompt = build_system_prompt(self)
        settings["system_prompt"] = "sha256:" + hashlib.sha256(prompt.encode()).hexdigest()[:12]
        return settings

    def to_json(self) -> str:
        """to_dict() as JSON."""
        return json_dumps(self.to_dict())


DEFAULT_CONFIG = AgentConfig()
