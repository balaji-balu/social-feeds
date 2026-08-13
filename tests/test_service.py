import asyncio
import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path

from social_feeds.pipeline import PipelineRunner, Post, Theme
from social_feeds.service import Credentials, PipelineService, RuntimeConfig


class Source:
    source_name = "fake"

    def __init__(self, posts=(), delay=0):
        self.posts = list(posts)
        self.delay = delay
        self.active = 0
        self.max_active = 0

    async def fetch(self):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return list(self.posts)
        finally:
            self.active -= 1


class FailingSource:
    source_name = "broken"

    async def fetch(self):
        raise RuntimeError("source unavailable")


class Llm:
    def __init__(self):
        self.calls = 0

    async def analyze(self, posts):
        self.calls += 1
        return [Theme("Theme", "Summary", [posts[0].source_id])]


class PipelineServiceTests(unittest.TestCase):
    def test_runtime_settings_and_credentials_are_separate(self):
        environment = {
            "SOCIAL_FEEDS_QUEUE_CAPACITY": "3",
            "SOCIAL_FEEDS_BATCH_SIZE": "2",
            "LLM_API_KEY": "secret",
        }

        config = RuntimeConfig.from_environment(environment)
        credentials = Credentials.from_environment(environment)

        self.assertEqual(config.queue_capacity, 3)
        self.assertEqual(config.batch_size, 2)
        self.assertEqual(credentials.llm_api_key, "secret")
        self.assertNotIn("secret", repr(config))

    def test_run_once_isolates_a_failing_source(self):
        with tempfile.TemporaryDirectory() as directory:
            good = Source([Post("good", "1", "Pain", "https://post", "pain")])
            good.source_name = "good"
            llm = Llm()
            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={"broken": FailingSource(), "good": good},
                llm=llm,
                pain_keywords=("pain",),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.source_failures, 1)
            self.assertEqual(summary.completed_batches, 1)
            self.assertEqual(llm.calls, 1)
            runner.close()

    def test_service_applies_bounded_polling_and_graceful_stop(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as directory:
                source = Source(
                    [Post("fake", "1", "Pain", "https://post", "pain")],
                    delay=0.005,
                )
                llm = Llm()
                runner = PipelineRunner(
                    database_path=Path(directory) / "pipeline.sqlite",
                    sources={"fake": source},
                    llm=llm,
                    pain_keywords=("pain",),
                )
                service = PipelineService(
                    runner,
                    RuntimeConfig(
                        queue_capacity=1,
                        poll_interval_seconds=0.005,
                        flush_interval_seconds=0.02,
                        shutdown_timeout_seconds=1,
                        batch_size=1,
                    ),
                    logger=logging.getLogger("test-service"),
                )
                task = asyncio.create_task(service.run())
                for _ in range(100):
                    if llm.calls:
                        break
                    await asyncio.sleep(0.005)
                service.stop()
                await task
                self.assertEqual(source.max_active, 1)
                self.assertEqual(llm.calls, 1)
                runner.close()

        asyncio.run(scenario())

    def test_startup_recovery_makes_interrupted_batches_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline.sqlite"
            first = PipelineRunner(database, {}, Llm(), ())
            first.close()
            connection = sqlite3.connect(database)
            connection.executescript(
                """
                INSERT INTO posts(source, source_id, title, url, text, status)
                VALUES ('fake', '1', 'Pain', 'https://post', 'pain', 'batched');
                INSERT INTO batches(status) VALUES ('processing');
                INSERT INTO batch_posts(batch_id, source, source_id) VALUES (1, 'fake', '1');
                INSERT INTO attempts(batch_id, status) VALUES (1, 'processing');
                """
            )
            connection.commit()
            connection.close()

            recovered = PipelineRunner(database, {}, Llm(), ())

            self.assertEqual(recovered.list_batch_statuses(), ["failed_retryable"])
            self.assertEqual(recovered.list_posts()[0].source_id, "1")
            recovered.close()


if __name__ == "__main__":
    unittest.main()
