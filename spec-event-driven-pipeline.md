## Problem Statement

People who monitor Hacker News and selected Reddit communities need a continuously running local service that discovers pain-point discussions, filters them deterministically, groups eligible posts into bounded batches, and produces durable themes and summaries. The service must survive restarts, avoid duplicate processing, respect source limits, and make failed work recoverable and auditable.

## Solution

Build a single-process Python asyncio pipeline that independently polls HN Algolia and configured subreddit RSS feeds, persists source state and discovered items in SQLite, applies configurable pain-keyword pre-filtering, batches candidates by count or time, sends one structured request through a provider-neutral OpenAI-compatible LLM adapter, validates the response locally, and commits durable theme results. The pipeline exposes operational summaries through structured logs and persisted batch state rather than requiring a hosted platform or agent framework.

## User Stories

1. As a product researcher, I want the service to poll configured HN queries and subreddit RSS feeds continuously, so that relevant new discussions are collected.
2. As an operator, I want source settings and pain keywords to be configurable, so that monitoring can change without changing pipeline logic.
3. As a product researcher, I want each item to retain its source identity and URL, so that generated themes remain auditable.
4. As an operator, I want duplicate items across polls and HN queries to collapse to one canonical item, so that results are not inflated.
5. As an operator, I want source progress and processing state persisted, so that restarts do not lose work or replay unbounded history.
6. As an operator, I want late-arriving items, RSS validators, timeouts, retries, backoff, jitter, OAuth, user-agent, and rate-limit requirements handled, so that integrations remain reliable and respectful.
7. As a product researcher, I want only keyword-matching candidates sent to the LLM, so that irrelevant content is excluded before paid inference.
8. As an operator, I want batches to flush at a configurable item threshold or after 15 minutes, so that low-volume sources still produce results.
9. As a product researcher, I want successful batches to produce validated themes, summaries, and example post references, so that the output is useful and traceable.
10. As an operator, I want transient failures retried with bounded policy and failed batches retained, so that work remains recoverable.
11. As an operator, I want startup recovery, bounded queues, graceful cancellation, structured logs, and per-batch summaries, so that local operation is observable and safe.
12. As a data steward, I want stored Reddit content minimized and deletable, so that retention follows source-policy requirements.
13. As a maintainer, I want the LLM provider isolated behind a narrow adapter and the deterministic pipeline framework-free, so that dependencies remain replaceable and proportionate.

## Implementation Decisions

- Implement one continuously running Python asyncio process with independent source pollers, a bounded queue, pre-filtering, batching, persistence, and LLM processing.
- Use SQLite as the source of truth for source watermarks, canonical items, batch membership, attempts, results, and recovery state.
- Use one HN watermark per configured query. Poll Algolia by date with overlap, page as needed, and deduplicate by stable source ID.
- Treat subreddit RSS as a bounded snapshot, using Atom IDs and timestamps, bounded recent-ID overlap, and ETag or Last-Modified validators when available.
- Advance source progress only after fetched items are durably recorded; do not mark items processed merely because they were fetched.
- Apply deterministic pain-keyword filtering before batching and inference.
- Flush non-empty batches at the configured item threshold or after 15 minutes; empty batches produce no LLM call.
- Use bounded asyncio queues, timeouts, explicit cancellation, graceful shutdown, and protection against overlapping polls.
- Persist batch and attempt state before inference. Commit validated results and item completion atomically; retain failures as retryable.
- Define a provider-neutral LLM adapter with provider, base URL, credentials, model, timeout, payload limits, and structured-output capability.
- Prefer native structured output where supported; otherwise parse JSON and validate locally, with at most one constrained repair request.
- Distinguish refusal, truncation, timeout, rate-limit, transport, malformed JSON, schema-validation, and quota failures.
- Retry transient transport and rate-limit failures using Retry-After or bounded exponential backoff with jitter; do not retry billing or quota failures automatically.
- Cap each item excerpt and total batch payload before inference.
- Keep the first implementation framework-free; do not adopt Mastra, Vercel Eve, Python-Eve, or another agent framework.
- Store only the minimum Reddit content needed for filtering, inference, and audit, with deletion or expiry handling.
- Use structured logs, durable per-batch summaries, startup recovery checks, and local operational configuration. Markdown is a projection, not the source of truth.
- Test through one end-to-end PipelineRunner boundary with injected source clients, SQLite state, clock, and LLM adapter. Verify persisted outcomes, retries, deduplication, and restart recovery rather than internal task arrangement.

## Testing Decisions

- Assert externally observable behavior at the PipelineRunner boundary and avoid coupling to private helpers or coroutine layout.
- Test HN pagination, overlap, per-query watermarks, duplicate IDs, late arrivals, Reddit Atom identity, validators, 304 responses, rate limits, and independent source failure.
- Test filtering, empty batches, threshold/time flushing, durable membership, interrupted batches, retryability, cancellation, structured output failures, repair limits, atomic commits, restart recovery, retention, and operational summaries.
- There is no existing application test prior art; establish these tests as the initial behavioral contract.

## Out of Scope

- Multi-agent orchestration or autonomous agent planning.
- Mastra, Vercel Eve, Python-Eve, or a hosted workflow platform.
- Distributed workers, hosted databases, production deployment infrastructure, or a web dashboard.
- Automatic posting back to Hacker News, Reddit, or any social network.
- Semantic moderation beyond the configured deterministic keyword pre-filter.
- Treating Reddit RSS as a durable cursor or inventing unsupported pagination semantics.

## Further Notes

The Wayfinder research concluded that SQLite plus bounded asyncio queues and a provider-neutral structured LLM adapter is the smallest suitable implementation boundary. Individual source adapters and the LLM provider should remain replaceable without changing the durable pipeline contract.
