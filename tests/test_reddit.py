import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from social_feeds.hn import HttpResponse
from social_feeds.pipeline import PipelineRunner, Theme
from social_feeds.reddit import RedditRssSource


ATOM_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>test</title>
  <id>https://www.reddit.com/r/test/.rss</id>
  <updated>2026-08-13T00:00:00Z</updated>
  <entry>
    <title>Painful workflow</title>
    <id>t3_post-1</id>
    <updated>2026-08-12T00:00:00Z</updated>
    <published>2026-08-12T00:00:00Z</published>
    <link href="https://reddit.test/post-1" />
    <content type="html">This is a very painful workflow.</content>
  </entry>
</feed>
"""


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def get(self, url, params, headers=None):
        self.requests.append((url, params, headers or {}))
        return self.responses.pop(0)


class FakeLlm:
    def __init__(self):
        self.calls = []

    async def analyze(self, posts):
        self.calls.append(list(posts))
        return [Theme("A Reddit theme", "A summary", [posts[0].source_id])]


class RedditRssSourceTests(unittest.TestCase):
    def test_parses_atom_and_reuses_validators_for_a_304_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline.sqlite"
            transport = FakeTransport(
                [
                    HttpResponse(200, {"ETag": '"abc"', "Last-Modified": "yesterday"}, ATOM_FEED),
                    HttpResponse(304, {}, ""),
                ]
            )
            source = RedditRssSource(
                subreddit="test",
                transport=transport,
                database_path=database,
            )
            llm = FakeLlm()
            runner = PipelineRunner(
                database_path=database,
                sources={"reddit:test": source},
                llm=llm,
                pain_keywords=("painful",),
            )

            first = asyncio.run(runner.run_once())
            second = asyncio.run(runner.run_once())

            self.assertEqual(first.discovered_posts, 1)
            self.assertEqual(second.discovered_posts, 0)
            self.assertEqual(len(llm.calls), 1)
            self.assertEqual(transport.requests[1][2]["If-None-Match"], '"abc"')
            self.assertEqual(transport.requests[1][2]["If-Modified-Since"], "yesterday")
            self.assertLessEqual(len(runner.list_posts()[0].text), 4000)
            runner.close()

    def test_expires_reddit_content_but_keeps_the_auditable_post_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline.sqlite"
            transport = FakeTransport([HttpResponse(200, {}, ATOM_FEED)])
            source = RedditRssSource(
                subreddit="test",
                transport=transport,
                database_path=database,
                retention_hours=48,
            )
            runner = PipelineRunner(
                database_path=database,
                sources={"reddit:test": source},
                llm=FakeLlm(),
                pain_keywords=("painful",),
            )
            asyncio.run(runner.run_once())

            source.expire_content(datetime(2026, 8, 15, tzinfo=timezone.utc))
            post = runner.list_posts()[0]

            self.assertEqual(post.source_id, "t3_post-1")
            self.assertEqual(post.text, "")
            self.assertEqual(post.title, "[expired]")
            runner.close()


if __name__ == "__main__":
    unittest.main()
