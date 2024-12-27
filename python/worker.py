import io
import json
import logging
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator

from commoncrawl import CCDownloader, Downloader
from rabbitmq import rabbitmq_channel

import config
import monitoring

worker_Monitor = monitoring.MonitoringModule()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        for record in WARCIterator(io.BytesIO(data)):
            if record.rec_type == "response":
                _text = trafilatura.extract(record.content_stream().read())
                # TODO: process text
    worker_Monitor.increment_counter("worker_consumed_batches")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    worker_Monitor.create_counter(
        "worker_consumed_batches",
        "Number of batches consumed by the worker"
    )
    start_http_server(9001)
    downloader = CCDownloader(
        config.config["BASE_URL"],
        logger,
        worker_Monitor

    )
    channel = rabbitmq_channel(config.config["QUEUE_NAME"])
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=config.config["QUEUE_NAME"],
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
