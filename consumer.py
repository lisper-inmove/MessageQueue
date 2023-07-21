from .msg_config import MsgConfig
from submodules.utils.sys_env import SysEnv


class Consumer:

    def get_consumer(self, msg_config):
        self.__redis_consumer(msg_config)
        self.__kafka_consumer(msg_config)
        return self.consumer

    def __redis_consumer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        if config.isAsync:
            from redis import asyncio as aioredis
            from .aioredis_mq.consumer import Consumer
            from .aioredis_mq.client import Client
            client = Client(
                client=aioredis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        else:
            import redis
            from .redis_mq.consumer import Consumer
            from .redis_mq.client import Client

            client = Client(
                client=redis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        self.consumer = Consumer(
            client,
            config,
        )

    def __kafka_consumer(self, config):
        if config.type != MsgConfig.KAFKA:
            return
        host = SysEnv.get("KAFKA_HOST")
        port = int(SysEnv.get("KAFKA_PORT"))
        config.bootstrap_servers = f"{host}:{port}"
        from .kafka_mq.consumer import Consumer
        self.consumer = Consumer(config)
