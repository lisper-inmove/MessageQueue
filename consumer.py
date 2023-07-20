from .msg_config import MsgConfig


class Consumer:

    def get_consumer(self, msg_config):
        self.__redis_consumer(msg_config)
        return self.consumer

    def __redis_consumer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        if config.isAsync:
            from redis import asyncio as aioredis
            from .aioredis_mq.consumer import Consumer
            from .aioredis_mq.client import Client
            client = Client(
                client=aioredis.StrictRedis(host=config.host, port=config.port),
                stream_name=config.stream_name
            )
        else:
            import redis
            from .redis_mq.consumer import Consumer
            from .redis_mq.client import Client

            client = Client(
                client=redis.StrictRedis(host=config.host, port=config.port),
                stream_name=config.stream_name
            )
        self.consumer = Consumer(
            client,
            from_now_on=config.from_now_on,
            block=config.block
        )
