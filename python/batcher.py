import logging

import commoncrawl
from prometheus_client import start_http_server
from rabbitmq import RabbitMQChannel

import config
import dedup_store
import index_processor
import monitoring

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main() -> None:
    """
    Main workflow:
    1. Run preprocessor.py. This will
       - Download all cluster.idx files specified in the configuration.
       - Combine the downloaded files into a single sorted file based on timestamps.
    2. Start Prometheus metrics HTTP server for monitoring.
    3. Process the combined index to batch and publish valid URLs to a RabbitMQ channel.
    """

    batcher_monitoring = monitoring.MonitoringModule()


    # Step 3: Start Prometheus metrics HTTP server
    logger.info("Step 3: Starting Prometheus metrics HTTP server on port 9000.")
    start_http_server(9000)

    # Step 4: Process the combined index
    logger.info("Step 4: Processing the combined index.")

    batcher_monitoring.create_counter(
        "batcher_pushed_batches",
        "Number of batches pushed to rabbitmq"
    )
    batcher_monitoring.create_counter(
        "batcher_failed_url_cdx_chunk_download",
        "Number of CDX chunks that failed to download due to http errors"
    )
    batcher_monitoring.create_gauge(
        "batcher_percentage_cluster_file_processed",
        "Percentage of records processed from combined cluster.idx"
    )
    batcher_monitoring.create_counter(
        "batcher_docs_considered_after_filtering",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )
    batcher_monitoring.create_counter(
        "batcher_duplicate_skipped_documents",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )
    batcher_monitoring.create_counter(
        "batcher_filtered_documents",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )

    channel = RabbitMQChannel(config.config["QUEUE_NAME"])
    downloader = commoncrawl.CCDownloader(
        config.config["BASE_URL"],
        logger,
        batcher_monitoring,
        "batcher_failed_url_cdx_chunk_download"
    )

    index_reader = commoncrawl.CSVIndexReader(
        f"{config.config['CLUSTER_DIR']}/{config.config['COMBINED_CLUSTER_FILE']}")

    deduplication_type = config.config.get("DEDUPLICATION_TYPE", config.DedupType.LATEST_URL)
    deduplication_store_type = config.config.get("DEDUP_STORE_TYPE", config.DedupStoreType.DICT)

    store = dedup_store.DictDedupDataStore() if deduplication_store_type == config.DedupStoreType.DICT else dedup_store.RedisDedupDataStore()

    if deduplication_type == config.DedupType.LATEST_URL:
        logger.info("Running URL batcher with latest URL keeping logic from duplicates")
        processor = index_processor.ProcessIndexLatestCrawl(
            config.config["QUEUE_NAME"],
            channel,
            downloader,
            config.config["BATCH_SIZE"],
            store,
            batcher_monitoring
        )
    elif deduplication_type == config.DedupType.UNIQUE_DIGEST_BASED:
        logger.info("Running URL batcher with unique digest based URL de-duplication")
        processor = index_processor.ProcessIndexUniqueDigest(
            config.config["QUEUE_NAME"],
            channel,
            downloader,
            config.config["BATCH_SIZE"],
            store,
            batcher_monitoring,
        )
    else:
        raise ValueError(f"Unknown deduplication type: {deduplication_type}")

    processor.process(index_reader)

if __name__ == "__main__":
    main()
