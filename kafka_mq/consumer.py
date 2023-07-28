from aiokafka import AIOKafkaConsumer
from aiokafka import TopicPartition
from kafka.errors import CommitFailedError
from kafka import TopicPartition
from submodules.utils.logger import Logger
from ..message import Message
from .helpr import Helper

logger = Logger()


class Consumer:

    def __init__(self, config):
        self.config = config
        self.isStart = False

    async def start(self):
        if self.isStart:
            return
        self.consumer = AIOKafkaConsumer(
            self.config.topic,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.groupName,
            # session_timeout_ms=60000,
            # heartbeat_interval_ms=20000
        )
        logger.info(f"kafka consumer start {self.config}")
        await self.consumer.start()
        self.isStart = True

    async def cleanup(self):
        logger.info(f"kafka consumer cleanup {self.config}")
        await self.consumer.stop()

    async def pull(self, count):
        await self.start()
        try:
            messages = await self.consumer.getmany(max_records=count)
            for topic, msgs in messages.items():
                for msg in msgs:
                    yield Message(
                        topic=topic,
                        value=msg
                    )
        except Exception as ex:
            logger.traceback(ex)
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.cleanup()

    async def ack(self, message):
        logger.info(f"kafka ack: {message.value}")
        topic = TopicPartition(
            self.config.topic,
            message.value.partition
        )
        try:
            await self.consumer.commit({topic: message.value.offset + 1})
        except CommitFailedError as ex:
            logger.warning(f"{message.value} -- {ex}")

    async def autoclaim(self):
        yield None
