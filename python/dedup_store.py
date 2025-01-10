import redis
from abc import ABC, abstractmethod

class BaseDedupDataStore(ABC):
    @abstractmethod
    def add(self, value):
        """
        Add a value to the dedup store if it is not already present.

        :param value: The value to add.
        :return: True if the value was added (not a duplicate), False otherwise.
        """
        pass

    @abstractmethod
    def contains(self, value):
        """
        Check if the value exists in the dedup store.

        :param value: The value to check.
        :return: True if the value exists, False otherwise.
        """
        pass

    @abstractmethod
    def remove(self, value):
        """
        Remove a value from the dedup store if it exists.

        :param value: The value to remove.
        :return: True if the value was removed, False if the value was not found.
        """
        pass

    @abstractmethod
    def get_all(self):
        """
        Retrieve all unique values from the dedup store.

        :return: A list of all unique values.
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Clear all values from the dedup store.

        :return: None
        """
        pass

class DictDedupDataStore(BaseDedupDataStore):
    def __init__(self):
        self.store = set()

    def add(self, value):
        if value not in self.store:
            self.store.add(value)
            return True
        return False

    def contains(self, value):
        return value in self.store

    def remove(self, value):
        if value in self.store:
            self.store.remove(value)
            return True
        return False

    def get_all(self):
        return list(self.store)

    def clear(self):
        self.store.clear()

class RedisDedupDataStore(BaseDedupDataStore):
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.store_name = "dedup_store"
        self.redis_client.delete(self.store_name)  # Clear any existing data in the Redis set

    def add(self, value):
        return self.redis_client.sadd(self.store_name, value) == 1

    def contains(self, value):
        return self.redis_client.sismember(self.store_name, value)

    def remove(self, value):
        return self.redis_client.srem(self.store_name, value) == 1

    def get_all(self):
        return list(self.redis_client.smembers(self.store_name))

    def clear(self):
        self.redis_client.delete(self.store_name)
