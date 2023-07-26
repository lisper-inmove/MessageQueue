from .mq_config import MQConfig
from submodules.utils.sys_env import SysEnv


class GroupConsumer:

    def get_group_consumer(self, config):
        self.__redis_group_consumer(config)
        return self.group_consumer

    def __redis_group_consumer(self, config):
        if config.type != MQConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        from redis import asyncio as aioredis
        from .aioredis_mq.group_consumer import GroupConsumer
        from .aioredis_mq.client import Client
        client = Client(
            client=aioredis.StrictRedis(host=host, port=port),
            config=config
        )
        self.group_consumer = GroupConsumer(
            client,
            config,
        )
