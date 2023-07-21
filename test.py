from redis import StrictRedis

from consumer import Consumer
from producer import Producer
from mq_config import MQConfig


class Test:

    def __init__(self):
        self.config = MQConfig(MQConfig.REDIS)
        self.config.host = "127.0.0.1"
        self.config.port = 6379
        self.config.streamName = "test-0001"

    def push(self, message):
        p = Producer().get_producer(self.config)
        p.push(message)

    def pull(self):
        consumer = Consumer().get_consumer(self.config)
        print(consumer.pull(1))


if __name__ == '__main__':
    test = Test()
    test.push({"name": "inmove"})
    print(test.pull())
