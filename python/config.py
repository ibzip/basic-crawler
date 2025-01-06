# config.py
from enum import Enum
from typing import Dict

class DedupStoreType(Enum):
    REDIS = "redis"
    DICT = "dict"

class DedupType(Enum):
    LATEST_URL = "latest_url"
    UNIQUE_DIGEST_BASED = "digest_based"

config = {
    "CRAWL_VERSION": [
        "CC-MAIN-2024-10",
        "CC-MAIN-2024-30",
        "CC-MAIN-2024-51",
        "CC-MAIN-2024-33",
        "CC-MAIN-2024-38",
        "CC-MAIN-2024-42",
        "CC-MAIN-2024-46",
        "CC-MAIN-2024-18",
        "CC-MAIN-2024-22",
        "CC-MAIN-2024-26"
    ],
    "BATCH_SIZE": 50,
    "BASE_URL": "https://data.commoncrawl.org",  # Replace with the actual base URL
    "DEDUP_STORE_TYPE": DedupStoreType.DICT, # ["dict", "redis"]
    "DEDUPLICATION_TYPE": DedupType.LATEST_URL, # ['latest_url', 'unique_digest_based']
    "QUEUE_NAME": "batches",
    "FILTERS": {
        "languages": ["eng"],
        "status": ["200"]
    },
    "CLUSTER_DIR": "cluster_files",
    "COMBINED_CLUSTER_FILE": "combined_cluster.idx",
    "WORKER_TEXT_SIZE_FILTER": [500, 1000000],
    "BATCHER_PROMETHEUS_SERVER_PORT": 9000,
    "WORKER_PROMETHEUS_SERVER_PORT": 9001, # note, haven't testes how this would work if multiple workers run the same
    # start_http_server command with the same port.
    "AWS_STORAGE_BUCKET": "test-bucket"

}



# Generate a dictionary for CLUSTER_IDX_URLS with the crawl version as the key
CLUSTER_IDX_URLS: Dict[str, str] = {
    version: f"{config['BASE_URL']}/cc-index/collections/{version}/indexes/cluster.idx"
    for version in config["CRAWL_VERSION"]
}
