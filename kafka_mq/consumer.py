from aiokafka import AIOKafkaConsumer
from submodules.utils.logger import Logger

logger = Logger()


class Consumer:

    def __init__(self, config):
        self.config = config
        self.consumer = AIOKafkaConsumer(
            self.config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.groupName
        )

    async def start(self):
        logger.info(f"kafka consumer start {self.config}")
        await self.consumer.start()

    async def cleanup(self):
        logger.info(f"kafka consumer cleanup {self.config}")
        await self.consumer.stop()

    async def consume(self):
        async for msg in self.consumer:
            yield msg

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.cleanup()
