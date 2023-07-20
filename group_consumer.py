from .msg_config import MsgConfig


class GroupConsumer:

    def get_group_consumer(self, msg_config):
        self.__redis_group_consumer(msg_config)
        return self.group_consumer

    def __redis_group_consumer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        if config.isAsync:
            from redis import asyncio as aioredis
            from .aioredis_mq.group_consumer import GroupConsumer
            from .aioredis_mq.client import Client
            client = Client(
                client=aioredis.StrictRedis(host=config.host, port=config.port),
                stream_name=config.stream_name
            )
        else:
            import redis
            from .redis_mq.group_consumer import GroupConsumer
            from .redis_mq.client import Client
            client = Client(
                client=redis.StrictRedis(host=config.host, port=config.port),
                stream_name=config.stream_name
            )
        self.group_consumer = GroupConsumer(
            client,
            config.group_name,
            config.consumer_name,
            block=config.block,
            from_now_on=config.from_now_on
        )
