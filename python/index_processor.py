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

    def publish_batch(self, batch) -> None:
        self.channel.basic_publish(
            exchange="",
            routing_key=self.rabbitmq_queue_name,
            body=json.dumps(batch),
        )
        self.batcher_monitor.increment_counter(
            "batcher_pushed_batches"
        )

    def _get_dedup_value(self, values):
        pass

    def _process_cdx_info(self, data):
        # Reverse data so that for duplicated surt_urls, the latest url comes to the top
        # In CDX files, same surt_urls are found consecutively in an ascending sorting order
        # Reverse the data to make the order descending
        # Then later we will filter the duplicates and pick only the top url in every set of duplicates
        data.reverse()

        local_found_urls = []

        for line in data:
            if line == "":
                continue
            values = line.split(" ")
            dedup_value = self._get_dedup_value(values)
            metadata = json.loads("".join(values[2:]))
            record = self._process_line_in_cdx_block(values, metadata, dedup_value)

            if len(record):
                local_found_urls.append(record)
        return local_found_urls


    def _process_line_in_cdx_block(self, values, metadata, dedup_value):
        surt_url = values[0]
        if self.dedup_data_store.contains(dedup_value):
            # We already have the same data from this url(based on digest value)
            # so ignore this url
            self.batcher_monitor.increment_counter(
                "batcher_duplicate_skipped_documents"
            )
            return {}

        self.dedup_data_store.add(dedup_value)

        if (
                "languages" in metadata and any(lang in metadata["languages"] for lang in config.config["FILTERS"]["languages"]) and
                metadata.get("status") in config.config["FILTERS"]["status"]
        ):
            self.batcher_monitor.increment_counter(
                "batcher_docs_considered_after_filtering"
            )
            return {
                "surt_url": surt_url,
                "timestamp": values[1],
                "metadata": metadata,
            }
        else:
            self.batcher_monitor.increment_counter(
                "batcher_filtered_documents"
            )
        return {}

    def process(self, index: commoncrawl.IndexReader) -> None:

        logger.info("Processing index now")

        total_rows_in_index = index.get_size()
        rows_processed = 0

        prev_prefix = None
        for cdx_chunk in index:

            crawl_version, url_prefix, _ = cdx_chunk[0].split(' ')
            if url_prefix != prev_prefix:
                # since we merged all the cluster.idx fies, and sorted the final file based on url_prefx/timestmap in descending order,
                # if there are prefix duplicates in the final file(arising from multiple individual cluster.idx files), they will
                # be lying together consecutively. That is why when the url prefix changes, we can be sure that we won't see this prefix again
                # and hence we can empty th state for this prefix.
                logger.info(f"starting new_prefix {url_prefix}")

                # We are going to process urls for a new prefix, so clear the dedup store
                # for previous prefix, saves memory
                self.dedup_data_store.clear()

            cdx_crawl_url = f"cc-index/collections/{crawl_version}/indexes/{cdx_chunk[1]}"
            data = self.downloader.download_and_unzip(
                cdx_crawl_url, int(cdx_chunk[2]), int(cdx_chunk[3])
            ).decode("utf-8")

            self.found_urls.extend(
                # We will need to maintain state for all unique surt_urls found under a prefix.
                # From that state of unique surt_urls, we would need to pick either:
                ## 1. the latest data-containing url based on timestamp
                ## 2. Unique data duplicates of a surt_url based on digest value associates with each occurrence of the surt_url
                # This proces shappens in the following function call.

                self._process_cdx_info(data.split("\n"))
            )
            #if url_prefix == "zw,org,talia)/2024/04/29/mostbet-az-90-kazino-azerbaycan-en-yuksek-bukmeyker-formal-sayt-%e6%b3%b0%e5%9b%bd%e5%a4%b4%e6%9d%a1%e6%96%b0%e9%97%bb-adiyaman-583":
            #    exit(0)

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
    def _get_dedup_value(self, values):
        return values[0]


class ProcessIndexUniqueDigest(ProcessIndexBase):
    def _get_dedup_value(self, values):
        return json.loads("".join(values[2:]))["digest"] # digest value from the metadata
