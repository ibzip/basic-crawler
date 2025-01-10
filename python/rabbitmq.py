from abc import ABC, abstractmethod
import os
import pika


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self, queue_name) -> None:
        self.channel = rabbitmq_channel(queue_name)

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
        )


def rabbitmq_channel(queue_name) -> pika.adapters.blocking_connection.BlockingChannel:

    connection = pika.BlockingConnection(
        pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel
