import json
import logging


import commoncrawl
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessIndexBase:
    def __init__(self, queue_name, channel, downloader, batch_size, dedup_data_store, batcher_monitor):
        self.rabbitmq_queue_name = queue_name
        self.channel = channel
        self.downloader = downloader

        self.batch_size = batch_size

        self.dedup_data_store = dedup_data_store
        self.batcher_monitor = batcher_monitor

        self.found_urls = []
        self.dedup_type = None

    def publish_batch(self, batch) -> None:
        logger.info(f"Pushing batch of size {len(batch)}")
        self.channel.basic_publish(
            exchange="",
            routing_key=self.rabbitmq_queue_name,
            body=json.dumps(batch),
        )
        self.batcher_monitor.increment_cunter(
            "batcher_pushed_batches"
        )

    def _process_cdx_info(self, cdx_chunk: str):
        raise NotImplementedError("Subclasses should implement this method")


    def _process_line_in_cdx_block(self, line):
        if line == "":
            return {}
        values = line.split(" ")
        metadata = json.loads("".join(values[2:]))
        surt_url = values[0]

        dedup_value = surt_url if self.dedup_type == config.DedupType.LATEST_URL else metadata['digest']

        if self.dedup_data_store.contains(dedup_value):
            # We already have the same data from this url(based on digest value)
            # so ignore this url
            return {}
        self.dedup_data_store.add(dedup_value)

        if (
                "languages" in metadata
                and "eng" in metadata["languages"]
                and metadata["status"] == "200"
        ):
            return {
                "surt_url": surt_url,
                "timestamp": values[1],
                "metadata": metadata,
            }
        return {}

    def process(self, index: commoncrawl.IndexReader) -> None:

        logger.info("Processing index now")

        total_rows_in_index = index.get_size()
        rows_processed = 0

        prev_prefix = None
        for cdx_chunk in index:
            #logger.info(cdx_chunk)
            crawl_version, url_prefix, _ = cdx_chunk[0].split(' ')

            if url_prefix != prev_prefix:
                # We are going to process urls for a new prefix, so clear the dedup store
                # for previous prefix
                self.dedup_data_store.clear()

            cdx_crawl_url = f"cc-index/collections/{crawl_version}/indexes/{cdx_chunk[1]}"
            data = self.downloader.download_and_unzip(
                cdx_crawl_url, int(cdx_chunk[2]), int(cdx_chunk[3])
            ).decode("utf-8")

            self.found_urls.extend(
                self._process_cdx_info(data)
            )

            if len(self.found_urls) >= self.batch_size:
                self.publish_batch(self.found_urls)
                self.found_urls = []

            prev_prefix = url_prefix
            rows_processed += 1

            self.batcher_monitor.set_gauge(
                "batcher_percentage_cluster_file_processed",
                (rows_processed / total_rows_in_index) * 100
            )

        if self.found_urls:
            self.publish_batch(self.found_urls)

class ProcessIndexLatestCrawl(ProcessIndexBase):
    def __index__(self, *args, **kwargs):
        self.dedup_type = config.DedupType.LATEST_URL
        super().__init__(*args, **kwargs)

    def _process_cdx_info(self, data):
        local_found_urls = []

        for line in data.split("\n"):
            record = self._process_line_in_cdx_block(line)
            if not len(record):
                self.batcher_monitor.increment_cunter(
                    "batcher_skipped_documents"
                )
            else:
                local_found_urls.append(record)
        return local_found_urls

class ProcessIndexUniqueDigest(ProcessIndexBase):
    def __index__(self, *args, **kwargs):
        self.dedup_type = config.DedupType.UNIQUE_DIGEST_BASED
        super().__init__(*args, **kwargs)


    def _process_cdx_info(self, data):
        local_found_urls = []

        for line in data.split("\n"):
            record = self._process_line_in_cdx_block(line, )
            if not len(record):
                self.batcher_monitor.increment_cunter(
                    "batcher_skipped_documents"
                )
            else:
                local_found_urls.append(record)
        return local_found_urls
