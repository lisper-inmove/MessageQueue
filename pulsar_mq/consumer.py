import aiopulsar
import pulsar
from pulsar import Timeout

from submodules.utils.logger import Logger
from ..message import Message

logger = Logger()


class Consumer:

    def __init__(self, config):
        self.config = config
        self.isStart = False

    async def start(self):
        if self.isStart:
            return
        client = await aiopulsar.connect(self.config.serverUrl)
        self.consumer = await client.subscribe(
            topic=self.config.topic,
            subscription_name=self.config.topic,
            consumer_name=self.config.consumerName,
            initial_position=pulsar.InitialPosition.Earliest,
            negative_ack_redelivery_delay_ms=10000,
            unacked_messages_timeout_ms=10000,
        )
        self.isStart = True

    async def pull(self, count):
        await self.start()
        try:
            msg = await self.consumer.receive(timeout_millis=1000)
            yield Message(
                value=msg
            )
        except Timeout as ex:
            logger.info(f"pulsar pull timeout: {ex}, {self.config}")
            yield None

    async def ack(self, msg):
        logger.info(f"pulsar ack: {msg}")
        await self.consumer.acknowledge(msg)
