import json

from aiokafka import AIOKafkaProducer
from submodules.utils.logger import Logger
from .helpr import Helper

logger = Logger()


class Producer:

    def __init__(self, config):
        self.config = config
        self.isStart = False

    async def start(self):
        if self.isStart:
            return
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers
        )
        logger.info(f"kafka producer start: {self.config}")
        await self.producer.start()
        self.isStart = True

    async def cleanup(self):
        logger.info(f"kafka producer cleanup: {self.config}")
        await self.producer.stop()

    async def push(self, message):
        await self.start()
        partition = Helper().partitioner(
            message.get('id'),
            self.config.maxPartition
        )
        message = json.dumps(message).encode()
        await self.producer.send(
            self.config.topic,
            message,
            partition=partition
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.cleanup()
