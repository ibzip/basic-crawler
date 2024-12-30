import datetime
import io
import hashlib
import json
import logging
import os
import uuid
import asyncio

from aio_pika import connect_robust, IncomingMessage
from aiobotocore.session import get_session
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator


import config
import monitoring
import commoncrawl

worker_Monitor = monitoring.MonitoringModule()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

My_WORKER_ID = uuid.uuid4().hex[:16]

s3_endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")

logger.info(f"{s3_endpoint}, {access_key}, {secret_key}")


def text_fits_length(txt):
    size_filter = config.config["WORKER_TEXT_SIZE_FILTER"]
    if len(txt) < size_filter[0] or len(txt) > size_filter[1]:
        return False
    return True

async def process_batch(downloader, batch):
    logger.info(f"Received message: {batch.body.decode()}")

    batch = json.loads(batch.body)
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    ) as s3_client:
        for item in batch:
            data = await downloader.download_and_unzip(
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
                        await s3_client.put_object(
                            Bucket="test-bucket",
                            Key=s3_key,
                            Body=file_body.encode("utf-8"),
                            ContentType="application/json"
                        )

                    worker_Monitor.increment_counter(f"worker_{My_WORKER_ID}_processed_documents")

async def main():
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

    downloader = commoncrawl.AsyncCCDownloader(
        config.config["BASE_URL"],
        logger,
        worker_Monitor,
        f"worker_{My_WORKER_ID}_warc_chunk_failed_download",
    )
    connection = await connect_robust(os.getenv("RABBITMQ_CONNECTION_STRING"))
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    # Declare or get the queue
    queue_name = "batches"
    queue = await channel.declare_queue(queue_name, durable=False)

    async def on_message(message: IncomingMessage):
        async with message.process():
            await process_batch(downloader, message)
            logger.info("batch processed")


    # Use Queue.iterator() to consume messages
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await on_message(message)

    await connection.close()



if __name__ == "__main__":
    asyncio.run(main())
