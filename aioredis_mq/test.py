from redis import StrictRedis

from redis_message_queue.client import Client
from redis_message_queue.consumer import Consumer
from redis_message_queue.producer import Producer


class Test:

    def __init__(self):
        redis_client = StrictRedis(
            host='127.0.0.1', port=6379
        )
        self.client = Client(
            redis_client,
            "test-00001"
        )

    def push(self, message):
        p = Producer(client=self.client)
        p.push(message)

    def pull(self):
        consumer = Consumer(client=self.client, fromNowOn=False)
        print(consumer.pull(1))


if __name__ == '__main__':
    test = Test()
    test.push({"name": "inmove"})
    print(test.pull())
