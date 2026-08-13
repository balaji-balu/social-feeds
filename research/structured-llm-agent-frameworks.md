# Research: structured LLM output and agent-framework fit

Research ticket for the social-feeds Wayfinder map. Sources are first-party documentation only; checked 2026-08-12.

## Findings

### Structured JSON, validation, and repair

- OpenAI Structured Outputs constrains responses to a supplied JSON Schema and supports Python Pydantic models in the official SDK. OpenAI describes this as type-safe output that avoids retries for formatting errors, but only a subset of JSON Schema is supported. The schema should therefore be deliberately small: an object containing `themes`, where each theme has required strings/arrays and `additionalProperties: false`.
  - https://developers.openai.com/api/docs/guides/structured-outputs
- Structured Outputs does not eliminate all failure cases. The response can be refused for safety reasons outside the schema, and the response can be incomplete when the finish reason is `length`. Both must be handled as explicit non-success states; do not mark a batch complete until the parsed object passes local Pydantic validation.
  - https://developers.openai.com/api/docs/guides/structured-outputs
- For an OpenAI-compatible provider, treat structured-output support as a capability, not an assumption. The adapter should expose a provider/model configuration and have a strict-schema path plus a fallback path that parses JSON and validates it locally. A repair prompt is a bounded fallback, not the primary correctness mechanism.

### Retries and payload limits

- OpenAI rate limits include requests/minute and tokens/minute (among other metrics). The official guidance is to honor a valid `Retry-After`; otherwise use bounded exponential backoff with jitter. Failed requests still count toward per-minute limits, and billing/quota errors should not be retried automatically.
  - https://developers.openai.com/api/docs/guides/rate-limits
  - https://developers.openai.com/api/docs/guides/error-codes
- A model’s context and output ceilings are model-specific. For example, the current GPT-4o mini page lists a 128,000-token context window and 16,384 maximum output tokens. The implementation should estimate/limit input size before calling the model and configure a finite output limit; do not hard-code these values as universal provider limits.
  - https://developers.openai.com/api/docs/models/gpt-4o-mini
- The proposed 15–20-item batch is operationally reasonable only if each item is truncated to a bounded title/body excerpt and the prompt budget is checked. Persist batch status and attempts before the call; commit results and mark items processed in one durable transaction only after validation succeeds. Failed batches remain pending for retry.

### Runtime and framework fit

- Mastra describes itself as a TypeScript framework, and its setup requires a Node/TypeScript project. Its workflows provide typed steps, retries/state/persistence, and observability, but adopting it would move this implementation from Python `asyncio` to a second runtime and require a bridge for the Python polling/RSS/SQLite process.
  - https://mastra.ai/docs
  - https://mastra.ai/ai-workflows
- Vercel `eve` is a filesystem-first framework for durable AI agents, with the repository identifying JavaScript/TypeScript and Vercel-oriented workflow concepts. Its repository explicitly says it is beta and that APIs, documentation, and behavior may change before general availability.
  - https://github.com/vercel/eve
- “Eve” is ambiguous: Python-Eve is an HTTP REST API framework powered by Flask, not an agent/workflow framework. It does not solve this pipeline’s asyncio scheduling, queueing, or LLM orchestration requirements.
  - https://docs.python-eve.org/en/stable/

## Design recommendations

1. Implement the service as a conventional single-process Python `asyncio` application. Use independent poller tasks, an `asyncio.Queue`, a batcher with count-or-time flush, SQLite for cursors/items/batches/results, and graceful cancellation.
2. Do not use Mastra or `eve` for the first implementation. They add a runtime/platform dependency without solving a current requirement. Reconsider an orchestration framework only when the system needs durable multi-step workflows, human approval, distributed workers, or framework-level tracing.
3. Define a narrow `LLMClient` adapter with `base_url`, `api_key`, `model`, timeout, max input characters/tokens, and a `supports_structured_output` capability. Target the common OpenAI-compatible request surface, but keep provider quirks behind the adapter.
4. Prefer strict JSON Schema/Pydantic parsing where supported. Handle refusal, timeout, rate-limit, transport, length truncation, malformed JSON, and schema validation separately. Retry transient transport/rate-limit failures with `Retry-After`/bounded exponential jitter; retry validation failures at most once with a repair request; retain the batch as pending after the retry budget is exhausted.
5. Use a compact schema such as `{"themes": [{"theme": str, "summary": str, "example_posts": [{"source_id": str, "url": str}]}]}`. Include stable source IDs in every example so results remain auditable without embedding full posts in the log.
6. Enforce payload limits before the LLM call: cap per-post title/body length, cap total batch characters/tokens, and split oversized batches while preserving the configured 15–20 item / 15-minute behavior as an upper bound rather than a guarantee.

## Resolution-ready conclusion

For this repository, choose Python `asyncio` plus a provider-neutral OpenAI-compatible adapter and local Pydantic validation. Use native Structured Outputs when available; otherwise validate and apply bounded repair. Mastra is TypeScript/Node-oriented, Vercel `eve` is TypeScript-oriented and beta, and Python-Eve is an unrelated REST framework. Neither is justified for this deterministic poll → filter → batch → one-LLM-call pipeline.
