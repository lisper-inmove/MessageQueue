from .msg_config import MsgConfig
from submodules.utils.sys_env import SysEnv


class Producer:

    def get_producer(self, msg_config):
        self.__redis_producer(msg_config)
        self.__kafka_producer(msg_config)
        self.__pulsar_producer(msg_config)
        return self.producer

    def __redis_producer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        if config.isAsync:
            from redis import asyncio as aioredis
            from .aioredis_mq.producer import Producer
            from .aioredis_mq.client import Client
            client = Client(
                client=aioredis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        else:
            import redis
            from .redis_mq.producer import Producer
            from .redis_mq.client import Client
            client = Client(
                client=redis.StrictRedis(host=host, port=port),
                streamName=config.streamName
            )
        self.producer = Producer(client, config)

    def __kafka_producer(self, config):
        if config.type != MsgConfig.KAFKA:
            return
        host = SysEnv.get("KAFKA_HOST")
        port = int(SysEnv.get("KAFKA_PORT"))
        config.bootstrap_servers = f"{host}:{port}"
        from .kafka_mq.producer import Producer
        self.producer = Producer(config)

    def __pulsar_producer(self, config):
        if config.type != MsgConfig.PULSAR:
            return
        host = SysEnv.get("PULSAR_HOST")
        port = int(SysEnv.get("PULSAR_PORT"))
        config.streamName = f"{SysEnv.get('PULSAR_TOPIC_PREFIX')}{config.streamName}"
        config.serverUrl = f"{host}:{port}"
        from .pulsar_mq.producer import Producer
        self.producer = Producer(config)
