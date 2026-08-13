import asyncio
import tempfile
import unittest
from pathlib import Path

from social_feeds.pipeline import PipelineRunner, Post, Theme


class FakeSource:
    def __init__(self, posts):
        self.posts = posts
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        return list(self.posts)


class FakeLlm:
    def __init__(self):
        self.calls = []

    async def analyze(self, posts):
        self.calls.append(list(posts))
        return [
            Theme(
                theme="Broken onboarding",
                summary="People struggle to start using the product.",
                example_posts=[posts[0].source_id],
            )
        ]


class FailingLlm:
    async def analyze(self, posts):
        raise RuntimeError("provider unavailable")


class PipelineRunnerTests(unittest.TestCase):
    def test_processes_a_candidate_into_a_durable_theme(self):
        with tempfile.TemporaryDirectory() as directory:
            source = FakeSource(
                [
                    Post(
                        source="fake",
                        source_id="post-1",
                        title="Onboarding is painful",
                        url="https://example.test/post-1",
                        text="I cannot figure out how to get started.",
                    )
                ]
            )
            llm = FakeLlm()
            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={"fake": source},
                llm=llm,
                pain_keywords=("painful", "cannot"),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.discovered_posts, 1)
            self.assertEqual(summary.candidates, 1)
            self.assertEqual(summary.completed_batches, 1)
            self.assertEqual(len(llm.calls), 1)
            self.assertEqual(runner.list_themes()[0].theme, "Broken onboarding")
            runner.close()

    def test_reopening_the_database_does_not_reprocess_the_same_post(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline.sqlite"
            post = Post(
                source="fake",
                source_id="post-1",
                title="Painful setup",
                url="https://example.test/post-1",
                text="Setup is painful.",
            )
            first_source = FakeSource([post])
            first_llm = FakeLlm()
            first = PipelineRunner(
                database_path=database,
                sources={"fake": first_source},
                llm=first_llm,
                pain_keywords=("painful",),
            )
            asyncio.run(first.run_once())
            first.close()

            second_source = FakeSource([post])
            second_llm = FakeLlm()
            second = PipelineRunner(
                database_path=database,
                sources={"fake": second_source},
                llm=second_llm,
                pain_keywords=("painful",),
            )

            summary = asyncio.run(second.run_once())

            self.assertEqual(summary.discovered_posts, 0)
            self.assertEqual(summary.completed_batches, 0)
            self.assertEqual(len(second_llm.calls), 0)
            self.assertEqual(len(second.list_themes()), 1)
            second.close()

    def test_failed_analysis_remains_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={
                    "fake": FakeSource(
                        [
                            Post(
                                source="fake",
                                source_id="post-1",
                                title="Painful setup",
                                url="https://example.test/post-1",
                                text="Setup is painful.",
                            )
                        ]
                    )
                },
                llm=FailingLlm(),
                pain_keywords=("painful",),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.failed_batches, 1)
            self.assertEqual(runner.list_batch_statuses(), ["failed_retryable"])
            runner.close()


if __name__ == "__main__":
    unittest.main()
