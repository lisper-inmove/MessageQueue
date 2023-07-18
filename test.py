from redis import StrictRedis

from consumer import Consumer
from producer import Producer
from msg_config import MsgConfig


class Test:

    def __init__(self):
        self.config = MsgConfig(MsgConfig.REDIS)
        self.config.host = "127.0.0.1"
        self.config.port = 6379
        self.config.stream_name = "test-0001"

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
