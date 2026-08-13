# Social-feed monitoring

This context describes the language used to discover pain points in Hacker News and configured Reddit communities and turn them into durable research themes.

## Language

**Source**:
A configured external feed, such as an HN query or subreddit, from which discussions are discovered.
_Avoid_: Channel, stream

**Post**:
A source discussion retained with its stable source identity, URL, and bounded content needed for analysis.
_Avoid_: Item, record

**Candidate**:
A post that passes the configured deterministic pain-keyword filter and is eligible for theme analysis.
_Avoid_: Match, lead

**Batch**:
A bounded group of candidates submitted together for one theme-analysis attempt.
_Avoid_: Job, run

**Theme**:
A recurring pain-point pattern identified across one or more analyzed posts.
_Avoid_: Topic, insight

**Operational summary**:
A durable account of source activity, batch outcomes, retries, and failures for an operating period.
_Avoid_: Report, dashboard
