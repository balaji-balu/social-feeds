# HN Algolia and Reddit feed polling research

Research for the Social Feeds Wayfinder ticket. Primary sources only; researched 2026-08-12.

## Findings

### HN Algolia Search API

- The HN Search API exposes `search_by_date`, with `page` pagination and response metadata including `nbPages` and `hitsPerPage`. It recommends requesting later pages explicitly when a query spans multiple pages. [HN Search API](https://hn.algolia.com/api)
- The API supports `numericFilters` on `created_at_i`; the official example uses `created_at_i>X` for items after a Unix-seconds watermark. [HN Search API](https://hn.algolia.com/api)
- HN hits expose a stable HN item identifier (`objectID` in Algolia results; the underlying HN API identifies items by unique integer `id`) and creation time (`created_at_i`; the underlying API calls the field `time`, Unix time). [HN Search API](https://hn.algolia.com/api), [Official HN API](https://github.com/HackerNews/API)
- The HN Algolia API documents a limit of 10,000 requests per hour from a single IP. The poller should stay comfortably below that limit with bounded concurrency, conservative polling, timeouts, retries, and backoff; the separate official HN API's lack of a stated limit does not override the Algolia service's published limit. [HN Search API](https://hn.algolia.com/api), [Official HN API](https://github.com/HackerNews/API)
- The documented interface is page-based, not a durable cursor/continuation-token interface. A timestamp alone is unsafe because records can share timestamps or become visible late.

### Reddit listings and RSS-relevant behavior

- Reddit’s official API documents listings as cursor-based: responses contain `after`/`before`, and the next request passes `after` as the fullname of the last item used as the anchor. Listings intentionally do not use page numbers because content changes frequently. [Reddit API documentation, listings](https://www.reddit.com/dev/api/#section_listings)
- Reddit fullnames are stable globally unique identifiers composed of a type prefix and base-36 ID; link posts use the `t3_` prefix. [Reddit API documentation, fullnames](https://www.reddit.com/dev/api/#section_fullnames)
- Reddit’s official documentation does not define a pagination or cursor contract for subreddit RSS/Atom endpoints. Therefore `.rss` should be treated as a bounded snapshot, not as a reliable continuation protocol. The Atom standard requires each entry to have a permanent `atom:id` and exactly one `atom:updated`, which gives the poller suitable identity and ordering fields when present. [RFC 4287, Atom](https://www.rfc-editor.org/rfc/rfc4287.html), [Example official Reddit feed endpoint](https://www.reddit.com/r/programming/.rss)
- Reddit’s current support documentation says OAuth is required for API access, unauthenticated traffic may be blocked, and eligible free Data API clients are limited to 100 queries per minute per OAuth client ID, averaged over a currently ten-minute window. It names `X-Ratelimit-Used`, `X-Ratelimit-Remaining`, and `X-Ratelimit-Reset` as the headers to monitor. [Reddit Data API Wiki](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki)
- Reddit’s terms require a truthful user agent and prohibit circumventing limits. They also require deletion of deleted Reddit content and recommend routinely deleting stored user data/content within 48 hours. This is relevant if the log stores raw Reddit bodies. [Reddit Data API Terms](https://redditinc.com/policies/data-api-terms), [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy)

### Conditional requests and asyncio

- HTTP conditional GETs use `If-None-Match`/ETag (and, where supplied, `If-Modified-Since`/Last-Modified) and may produce `304 Not Modified`; this is the standards-based optimization to use for repeated RSS fetches. It is optional because the source may not emit validators. [RFC 9110, conditional requests](https://www.rfc-editor.org/rfc/rfc9110.html#name-if-none-match)
- Python’s official asyncio documentation describes asyncio as appropriate for IO-bound network work and provides queues, tasks, task groups, timeouts, cancellation, and synchronization primitives. [asyncio](https://docs.python.org/3/library/asyncio.html), [Coroutines and Tasks](https://docs.python.org/3/library/asyncio-task.html)

## Explicit design recommendations

1. Use SQLite, not a single JSON cursor file. Keep a `sources`/watermark table, a deduplicating `items` table keyed by a normalized source-qualified ID, and durable batch/result tables.
2. For each HN query, poll `search_by_date` with `tags=story`, a configurable `hitsPerPage`, and a timestamp overlap. Page through all pages needed to cross the previous watermark, then filter/deduplicate locally by `objectID`. Advance the query watermark only after the fetched page set has been durably recorded.
3. Keep one HN watermark per configured query (`hn:<query>`), plus global item deduplication. Do not use one shared HN cursor across queries.
4. For Reddit RSS, poll each subreddit URL independently (`reddit:<subreddit>`), parse Atom `id`, `updated`, `published` when available, and retain a bounded recent-ID overlap window. Do not invent an `after` cursor for RSS. If reliable cursor semantics become a requirement, switch that source to Reddit’s OAuth JSON `/r/<subreddit>/new` listing and persist its `after` fullname.
5. Never advance a source watermark merely because an item was fetched. Store fetched items first; mark them eligible/processed only after pre-filtering and a successful, committed LLM result. Failed batches must remain retryable.
6. Use a bounded `asyncio.Queue` to provide backpressure. Run independent source pollers, a pre-filter consumer, and one batcher/LLM worker (or an explicitly bounded number of LLM workers). Wrap every network call in a timeout and use cancellation-safe graceful shutdown.
7. Poll at a conservative interval configurable at the top of the file. Keep HN Algolia traffic below its published 10,000-requests/hour/IP limit. Add jitter, exponential backoff, and `Retry-After` handling for 429/5xx responses. For Reddit, enforce the documented OAuth/user-agent requirements and monitor the three `X-Ratelimit-*` headers; the ten configured subreddits make one request per interval preferable to aggressive pagination.
8. Store ETag and Last-Modified per RSS URL when returned and send validators on later requests; treat 304 as “no new entries.” Still perform ID-based deduplication because feed ordering and retention are not a documented durable cursor contract.
9. Store only the minimum Reddit content needed for the LLM and output. Include source URL/ID, timestamps, title, and a bounded text excerpt; add a retention/deletion path for removed content instead of treating the SQLite log as permanent raw-content storage.
10. Keep the LLM adapter provider-neutral and validate the returned JSON against the required `{theme, summary, example_posts}` shape before committing a batch. Use the configured HN queries and subreddit list for acquisition, and `PAIN_KEYWORDS` only as the deterministic pre-filter.

## Resolution-ready conclusion

Use HN Algolia timestamp-overlap polling with one watermark per query and local ID deduplication. Use Reddit RSS as independently polled, bounded snapshots with Atom IDs, validators, and overlap deduplication; do not model RSS as cursor-paginated. If Reddit reliability or historical catch-up becomes important, use the official OAuth JSON listing and its `after` fullname cursor instead. SQLite plus bounded asyncio queues, timeouts, jittered backoff, and durable batch state is the appropriate implementation boundary. No Mastra/Eve agent framework is needed for the polling pipeline; isolate the single structured LLM call behind an adapter.
