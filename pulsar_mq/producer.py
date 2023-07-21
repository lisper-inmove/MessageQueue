import json

import pulsar
from submodules.utils.logger import Logger

logger = Logger()


class Producer:

    def __init__(self, config):
        self.config = config
        client = pulsar.Client(self.config.serverUrl)
        self.producer = client.create_producer(
            self.config.streamName,
            block_if_queue_full=True,
            batching_enabled=True,
            batching_max_publish_delay_ms=10
        )

    def send_callback(self, res, msg):
        logger.info(f"pulsar producer callback: {res} {msg}")

    async def push(self, message):
        message = json.dumps(message).encode()
        self.producer.send_async(message, self.send_callback)

    async def cleanup(self):
        pass
