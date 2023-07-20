from .msg_config import MsgConfig
from submodules.utils.sys_env import SysEnv


class GroupConsumer:

    def get_group_consumer(self, msg_config):
        self.__redis_group_consumer(msg_config)
        return self.group_consumer

    def __redis_group_consumer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        if config.isAsync:
            from redis import asyncio as aioredis
            from .aioredis_mq.group_consumer import GroupConsumer
            from .aioredis_mq.client import Client
            client = Client(
                client=aioredis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        else:
            import redis
            from .redis_mq.group_consumer import GroupConsumer
            from .redis_mq.client import Client
            client = Client(
                client=redis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        self.group_consumer = GroupConsumer(
            client,
            config.groupName,
            config.consumerNname,
            block=config.block,
            fromNowOn=config.fromNowOn
        )
