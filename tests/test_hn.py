import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from social_feeds.hn import HnAlgoliaSource, HttpResponse
from social_feeds.pipeline import PipelineRunner, Theme


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def get(self, url, params, headers=None):
        self.requests.append((url, params, headers or {}))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeLlm:
    async def analyze(self, posts):
        return [Theme("A theme", "A summary", [posts[0].source_id])]


def response(hits, nb_pages=1, status=200, headers=None):
    return HttpResponse(
        status=status,
        headers=headers or {},
        body=json.dumps({"hits": hits, "nbPages": nb_pages}),
    )


class HnAlgoliaSourceTests(unittest.TestCase):
    def test_paginates_with_overlap_and_persists_one_cursor_per_query(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                [
                    response(
                        [
                            {"objectID": "1", "title": "Pain one", "url": "https://hn/1", "created_at_i": 100},
                            {"objectID": "2", "title": "Pain two", "url": "https://hn/2", "created_at_i": 101},
                        ],
                        nb_pages=2,
                    ),
                    response(
                        [{"objectID": "3", "title": "Pain three", "url": "https://hn/3", "created_at_i": 102}]
                    ),
                ]
            )
            source = HnAlgoliaSource(
                query="pain",
                transport=transport,
                database_path=Path(directory) / "pipeline.sqlite",
                overlap_seconds=60,
                retry_delay=0,
            )
            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={"hn:pain": source},
                llm=FakeLlm(),
                pain_keywords=("pain",),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.discovered_posts, 3)
            self.assertEqual(len(transport.requests), 2)
            self.assertEqual(transport.requests[0][1]["page"], "0")
            self.assertEqual(transport.requests[1][1]["page"], "1")
            self.assertEqual(source.cursor, 102)
            runner.close()

    def test_retries_rate_limit_and_does_not_advance_cursor_before_successful_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                [
                    response([], status=429, headers={"Retry-After": "0"}),
                    response([{"objectID": "1", "title": "Pain", "url": "https://hn/1", "created_at_i": 100}]),
                ]
            )
            source = HnAlgoliaSource(
                query="pain",
                transport=transport,
                database_path=Path(directory) / "pipeline.sqlite",
                retry_delay=0,
            )
            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={"hn:pain": source},
                llm=FakeLlm(),
                pain_keywords=("pain",),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.discovered_posts, 1)
            self.assertEqual(source.cursor, 100)
            self.assertEqual(len(transport.requests), 2)
            runner.close()


if __name__ == "__main__":
    unittest.main()
