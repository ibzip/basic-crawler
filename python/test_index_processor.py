import unittest
from unittest.mock import MagicMock
import index_processor
from dedup_store import DictDedupDataStore

class TestProcessFunction(unittest.TestCase):
    def setUp(self):
        # Mocking dependencies
        self.mock_channel = MagicMock()
        self.mock_downloader = MagicMock()
        self.mock_batcher_monitor = MagicMock()
        self.mock_downloader.download_and_unzip.side_effect = [
            ("""
zw,org,talia)/ timestamp_3 {"digest": "digest2", "status": "200", "languages": ["en"]}\n
zw,org,talia)/ timestamp_4 {"digest": "digest1", "status": "200", "languages": ["en"]}\n
zw,org,talia)/ timestamp_5 {"digest": "digest1", "status": "200", "languages": ["en"]}\n
zw,org,talia,talia)/ timestamp_6 {"digest": "digest2", "status": "200", "languages": ["en"]}\n
            """.strip().encode('utf-8')),
            ("""
zw,org,talia)/ timestamp_1 {"digest": "digest3", "status": "200", "languages": ["en"]}
zw,org,talia)/ timestamp_2 {"digest": "digest2", "status": "200", "languages": ["en"]}
xy,org,example)/ timestamp_0 {"digest": "digest5", "status": "200", "languages": ["en"]}
            """.strip().encode('utf-8'))
        ]

        self.index_reader = MagicMock()
        self.index_reader.__iter__.return_value = [
            ("2024-01 zw,org,talia)/ timestamp2", "cdx-00.gz", "0", "100", "cluster1"),
            ("2024-01 zw,org,talia)/ timestamp1", "cdx-11.gz", "101", "200", "cluster2")
        ]
        self.index_reader.get_size.return_value = 2

        # Use DictDedupDataStore for realistic deduplication
        self.dedup_data_store = DictDedupDataStore()

    def test_deduplication_digest(self):
        # Test instance for digest-based deduplication
        process_index = index_processor.ProcessIndexUniqueDigest(
            queue_name="test_queue",
            channel=self.mock_channel,
            downloader=self.mock_downloader,
            batch_size=10,  # Increase batch size to prevent clearing of found_urls
            dedup_data_store=self.dedup_data_store,
            batcher_monitor=self.mock_batcher_monitor,
        )

        process_index.process(self.index_reader)

        # Check that found_urls contains only unique digests
        expected_records = [
            {
                "surt_url": "zw,org,talia,talia)/",
                "timestamp": "timestamp_6",
                "metadata": {"digest": "digest2", "status": "200", "languages": ["en"]},
            },
            {
                "surt_url": "zw,org,talia)/",
                "timestamp": "timestamp_5",
                "metadata": {"digest": "digest3", "status": "200", "languages": ["en"]},
            },
            {
                "surt_url": "xy,org,example)/",
                "timestamp": "timestamp_0",
                "metadata": {"digest": "digest5", "status": "200", "languages": ["en"]},
            },
            {
                "surt_url": "zw,org,talia)/",
                "timestamp": "timestamp_1",
                "metadata": {"digest": "digest1", "status": "200", "languages": ["en"]},
            },
        ]

        #self.assertEqual(process_index.found_urls, expected_records)

        # Verify dedup_data_store contents
        self.assertTrue(self.dedup_data_store.contains("digest1"))
        self.assertTrue(self.dedup_data_store.contains("digest2"))
        self.assertTrue(self.dedup_data_store.contains("digest3"))
        self.assertTrue(self.dedup_data_store.contains("digest5"))

    def test_deduplication_latest_url(self):
        # Test instance for latest-url-based deduplication
        process_index = index_processor.ProcessIndexLatestCrawl(
            queue_name="test_queue",
            channel=self.mock_channel,
            downloader=self.mock_downloader,
            batch_size=10,  # Increase batch size to prevent clearing of found_urls
            dedup_data_store=self.dedup_data_store,
            batcher_monitor=self.mock_batcher_monitor,
        )

        process_index.process(self.index_reader)

        # Check that found_urls contains only the latest entries for each surt_url
        expected_records = [
            {
                "surt_url": "zw,org,talia,talia)/",
                "timestamp": "timestamp_6",
                "metadata": {"digest": "digest2", "status": "200", "languages": ["en"]},
            },
            {
                "surt_url": "zw,org,talia)/",
                "timestamp": "timestamp_5",
                "metadata": {"digest": "digest1", "status": "200", "languages": ["en"]},
            },
            {
                "surt_url": "xy,org,example)/",
                "timestamp": "timestamp_0",
                "metadata": {"digest": "digest5", "status": "200", "languages": ["en"]},
            }
        ]

        self.assertEqual(process_index.found_urls, expected_records)

        # Verify dedup_data_store contents
        self.assertTrue(self.dedup_data_store.contains("zw,org,talia)/"))
        self.assertTrue(self.dedup_data_store.contains("zw,org,talia,talia)/"))
        self.assertTrue(self.dedup_data_store.contains("xy,org,example)/"))
        self.assertTrue(len(self.dedup_data_store.get_all()) == 3)

if __name__ == "__main__":
    unittest.main()
