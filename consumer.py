from .mq_config import MQConfig
from submodules.utils.sys_env import SysEnv


class Consumer:

    def get_consumer(self, config):
        self.__redis_consumer(config)
        self.__kafka_consumer(config)
        self.__pulsar_consumer(config)
        return self.consumer

    def __redis_consumer(self, config):
        if config.type != MQConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        from redis import asyncio as aioredis
        from .aioredis_mq.consumer import Consumer
        from .aioredis_mq.client import Client
        client = Client(
            client=aioredis.StrictRedis(host=host, port=port),
        )
        self.consumer = Consumer(
            client,
            config,
        )

    def __kafka_consumer(self, config):
        if config.type != MQConfig.KAFKA:
            return
        host = SysEnv.get("KAFKA_HOST")
        port = int(SysEnv.get("KAFKA_PORT"))
        config.bootstrap_servers = f"{host}:{port}"
        from .kafka_mq.consumer import Consumer
        self.consumer = Consumer(config)

    def __pulsar_consumer(self, config):
        if config.type != MQConfig.PULSAR:
            return
        host = SysEnv.get("PULSAR_HOST")
        port = int(SysEnv.get("PULSAR_PORT"))
        config.topic = f"{SysEnv.get('PULSAR_TOPIC_PREFIX')}{config.topic}"
        config.serverUrl = f"{host}:{port}"
        from .pulsar_mq.consumer import Consumer
        self.consumer = Consumer(config)
