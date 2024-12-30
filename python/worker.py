import datetime
import io
import hashlib
import json
import logging
import os
import uuid

import boto3
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

My_WORKER_ID = uuid.uuid4().hex[:16]


s3_endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")

print(f"{s3_endpoint}, {access_key}, {secret_key}")
s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)


def text_fits_length(txt):
    size_filter = config.config["WORKER_TEXT_SIZE_FILTER"]
    if len(txt) < size_filter[0] or len(txt) > size_filter[1]:
        return False
    return True

def process_batch(downloader: Downloader, ch, method, _properties, body):
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
                if not _text or not text_fits_length(_text):
                    worker_Monitor.increment_counter(f"worker_{My_WORKER_ID}_filtered_documents")
                else:
                    # Parse metadata
                    url_prefix = item.get("url_prefix", "unknown_prefix")
                    timestamp = item.get("timestamp", "unknown_timestamp")
                    metadata = item.get("metadata", {})

                    # Extract year, month, and day from timestamp
                    try:
                        dt = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
                        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
                    except ValueError:
                        year, month, day = "unknown", "unknown", "unknown"

                    # Generate file name and hierarchy
                    url = metadata.get("url", "unknown_url")
                    safe_filename = hashlib.sha256(url.encode()).hexdigest()[:16] + ".txt"
                    s3_key = os.path.join(year, month, day, url_prefix, safe_filename)

                    # Prepare content to store in file
                    file_content = {
                        "metadata": {
                            "url_prefix": url_prefix,
                            "surt_url": item.get("surt_url", "unknown_surt_url"),
                            "timestamp": timestamp,
                            "url": url,
                            **metadata
                        },
                        "text": _text
                    }

                    file_body = json.dumps(file_content, indent=4)
                    # Upload the text to S3
                    s3_client.put_object(
                        Bucket="test-bucket",
                        Key=s3_key,
                        Body=file_body,
                        ContentType="application/json"
                    )

                worker_Monitor.increment_counter(f"worker_{My_WORKER_ID}_processed_documents")
                # TODO: process text
    logger.info("done processing batch")
    worker_Monitor.increment_counter(f"worker_{My_WORKER_ID}_consumed_batches")
    ch.basic_ack(delivery_tag=method.delivery_tag)



def main() -> None:
    worker_Monitor.create_counter(
        f"worker_{My_WORKER_ID}_consumed_batches",
        "Number of batches consumed by the worker"
    )
    worker_Monitor.create_counter(
        f"worker_{My_WORKER_ID}_warc_chunk_failed_download",
        "Number of WARC chunks failed to download due to http failure"
    )
    worker_Monitor.create_counter(
        f"worker_{My_WORKER_ID}_filtered_documents",
        "Number of text documents filtered out due to length filter"
    )
    worker_Monitor.create_counter(
        f"worker_{My_WORKER_ID}_processed_documents",
        "Number of text documents processed by the worker(filtered and non-filtered)"
    )
    start_http_server(9001)
    downloader = CCDownloader(
        config.config["BASE_URL"],
        logger,
        worker_Monitor,
        f"worker_{My_WORKER_ID}_warc_chunk_failed_download",
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
