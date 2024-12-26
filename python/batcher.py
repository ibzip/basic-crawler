import logging
import os
import subprocess
import time
from typing import Dict

import commoncrawl
from prometheus_client import start_http_server
from rabbitmq import RabbitMQChannel

import config
import dedup_store
import index_processor
import monitoring

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

batcher_monitoring = monitoring.MonitoringModule()


def download_cluster_files(cluster_urls: Dict[str, str], output_dir: str = "cluster_files") -> None:
    """
    Download all cluster.idx files using `wget`, but skip files that already exist.

    :param cluster_urls: Dictionary where keys are version names and values are URLs to download.
    :param output_dir: Directory where the downloaded files will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)

    for version, url in cluster_urls.items():
        output_file = os.path.join(output_dir, f"{version}_cluster.idx")

        if os.path.exists(output_file):
            logger.info(f"File already exists, skipping download: {output_file}")
            continue

        logger.info(f"Downloading {url} -> {output_file}")

        # Use wget to download the file directly to disk
        try:
            subprocess.run([
                "wget", "-q", "-O", output_file, url
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to download {url}: {e}")
        time.sleep(2)

def combine_cluster_files(cluster_urls: Dict[str, str], output_dir: str = "cluster_files", combined_output: str = "combined_cluster.idx") -> None:
    """
    Combine all cluster.idx files into one output file, appending the cluster version.
    Then sort the combined file by url_prefix (column 2) and timestamp (column 3) in descending order.

    :param cluster_urls: Dictionary where keys are version names and values are URLs to download.
    :param output_dir: Directory containing the downloaded cluster.idx files.
    :param combined_output: Path to the combined output file.
    """
    temp_combined = combined_output + ".tmp"

    output_file = f"{output_dir}/{combined_output}"

    if os.path.exists(output_file):
        logger.info(f"Combined file {output_file} already exists, skipping combining and sorting.")
        return

    # Combine all cluster files with the cluster version prepended
    with open(temp_combined, "w") as temp_file:
        for version, _ in cluster_urls.items():
            full_input_path = os.path.join(output_dir, f"{version}_cluster.idx")
            if os.path.exists(full_input_path):
                subprocess.run([
                    "awk", f"{{print \"{version} \" \t $0}}", full_input_path
                ], stdout=temp_file, check=True)

    # Sort the combined file by url_prefix (column 2) and timestamp (column 3) in descending order
    sorted_combined = combined_output + ".sorted"
    subprocess.run([
        "sort", "-k2,2r", "-k3,3r", temp_combined, "-o", sorted_combined
    ], check=True)

    # Move the sorted file to the final output path
    subprocess.run(["mv", sorted_combined, output_file], check=True)
    os.remove(temp_combined)

    logger.info(f"Combined and sorted cluster files into {output_file}")


def main() -> None:
    """
    Main workflow:
    1. Download all cluster.idx files specified in the configuration.
    2. Combine the downloaded files into a single sorted file based on timestamps.
    3. Start Prometheus metrics HTTP server for monitoring.
    4. Process the combined index to batch and publish valid URLs to a RabbitMQ channel.
    """
    # Step 1: Download all cluster.idx files
    logger.info("Step 1: Downloading cluster.idx files.")
    download_cluster_files(config.CLUSTER_IDX_URLS)

    # Step 2: Combine all cluster.idx files into one
    logger.info("Step 2: Combining cluster.idx files into one.")
    combined_cluster_file = "combined_cluster.idx"
    output_dir: str = "cluster_files"

    combine_cluster_files(
        config.CLUSTER_IDX_URLS,
        output_dir=output_dir,
        combined_output=combined_cluster_file,
    )

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
        "batcher_considered_docs_after_filtering",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )
    batcher_monitoring.create_counter(
        "batcher_dups_skipped_documents",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )
    batcher_monitoring.create_counter(
        "batcher_filtered_documents",
        "Number of documents filtered due to language criteria or url inaccessibility"
    )

    channel = RabbitMQChannel(config.config["QUEUE_NAME"])
    downloader = commoncrawl.CCDownloader(config.config["BASE_URL"], logger, batcher_monitoring)
    index_reader = commoncrawl.CSVIndexReader(f"{output_dir}/{combined_cluster_file}")

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
