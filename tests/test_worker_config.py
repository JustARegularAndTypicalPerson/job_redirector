import unittest

from worker_config import load_worker_config


class WorkerConfigTests(unittest.TestCase):
    def test_requires_redis_url(self):
        with self.assertRaisesRegex(ValueError, "REDIS_URL is required"):
            load_worker_config({})

    def test_loads_redis_and_optional_logging_settings(self):
        config = load_worker_config(
            {
                "REDIS_URL": "redis://localhost:6379/0",
                "REDIS_PASSWORD": "local-test-password",
                "LOGTAIL_TOKEN": "test-token",
                "INGESTION_HOST": "https://logs.example.test",
            }
        )

        self.assertEqual(config.redis_url, "redis://localhost:6379/0")
        self.assertEqual(config.redis_password, "local-test-password")
        self.assertEqual(config.logtail_token, "test-token")
        self.assertEqual(config.ingestion_host, "https://logs.example.test")

    def test_optional_logging_can_be_disabled(self):
        config = load_worker_config({"REDIS_URL": "redis://localhost:6379/0"})

        self.assertIsNone(config.logtail_token)
        self.assertIsNone(config.ingestion_host)

    def test_rejects_partial_logging_configuration(self):
        with self.assertRaisesRegex(ValueError, "Set both LOGTAIL_TOKEN and INGESTION_HOST"):
            load_worker_config(
                {
                    "REDIS_URL": "redis://localhost:6379/0",
                    "LOGTAIL_TOKEN": "test-token",
                }
            )


if __name__ == "__main__":
    unittest.main()
