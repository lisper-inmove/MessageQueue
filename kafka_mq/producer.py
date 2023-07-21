import json

from aiokafka import AIOKafkaProducer
from submodules.utils.logger import Logger

logger = Logger()


class Producer:

    def __init__(self, config):
        self.config = config
        self.producer = AIOKafkaProducer(
            bootstrap_servers=config.bootstrap_servers
        )
        self.isStart = False

    async def start(self):
        if self.isStart:
            return
        logger.info(f"kafka producer start: {self.config}")
        await self.producer.start()
        self.isStart = True

    async def cleanup(self):
        logger.info(f"kafka producer cleanup: {self.config}")
        await self.producer.stop()

    async def push(self, message):
        await self.start()
        message = json.dumps(message).encode()
        await self.producer.send(
            self.config.topic,
            message
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.cleanup()
