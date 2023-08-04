from .mq_config import MQConfig
from submodules.utils.sys_env import SysEnv


class Producer:

    async def get_producer(self, config):
        await self.__redis_producer(config)
        await self.__kafka_producer(config)
        await self.__pulsar_producer(config)
        await self.__redis_cluster_producer(config)
        return self.producer

    async def __redis_producer(self, config):
        if config.type != MQConfig.REDIS:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        from redis import asyncio as aioredis
        from .aioredis_mq.producer import Producer
        from .aioredis_mq.client import Client
        client = Client(
            client=await aioredis.StrictRedis(host=host, port=port),
            config=config
        )
        self.producer = Producer(client, config)

    async def __redis_cluster_producer(self, config):
        if config.type != MQConfig.REDIS_CLUSTER:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        from .aioredis_mq.producer import Producer
        from .aioredis_mq.redis_cluster import AredisCluster
        from .aioredis_mq.client import Client
        startup_nodes_config = SysEnv.get("REDIS_STARTUP_NODES").split(":")
        start_up_nodes = []
        for config in startup_nodes_config:
            config = config.split(",")
            start_up_nodes.append((config[0], config[1]))
        obj = AredisCluster(
            host=host,
            port=port,
            startup_nodes=start_up_nodes,
        )

        await obj.connect()
        client = Client(obj, config)
        self.producer = Producer(client, config)

    async def __kafka_producer(self, config):
        if config.type != MQConfig.KAFKA:
            return
        host = SysEnv.get("KAFKA_HOST")
        port = int(SysEnv.get("KAFKA_PORT"))
        config.bootstrap_servers = f"{host}:{port}"
        from .kafka_mq.producer import Producer
        self.producer = Producer(config)
        await self.producer.start()

    async def __pulsar_producer(self, config):
        if config.type != MQConfig.PULSAR:
            return
        host = SysEnv.get("PULSAR_HOST")
        port = int(SysEnv.get("PULSAR_PORT"))
        config.topic = f"{SysEnv.get('PULSAR_TOPIC_PREFIX')}{config.topic}"
        config.serverUrl = f"{host}:{port}"
        from .pulsar_mq.producer import Producer
        self.producer = Producer(config)
