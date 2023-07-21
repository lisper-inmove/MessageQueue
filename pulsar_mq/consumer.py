import aiopulsar
import pulsar


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
        )
        self.isStart = True

    async def pull(self, count):
        await self.start()
        while True:
            msg = await self.consumer.receive()
            yield msg

    async def ack(self, msg):
        await self.consumer.acknowledge(msg)
