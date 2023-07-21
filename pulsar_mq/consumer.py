import aiopulsar
import pulsar


class Consumer:

    def __init__(self, config):
        self.config = config

    async def pull(self, count):
        async with aiopulsar.connect(self.config.serverUrl) as client:
            async with client.subscribe(
                    topic=self.config.topic,
                    subscription_name=self.config.streamName,
                    consumer_name=self.config.consumerName,
                    initial_position=pulsar.InitialPosition.Earliest,
            ) as consumer:
                while True:
                    msg = await consumer.receive()
                    yield msg.value()
