from .mq_config import MQConfig
from submodules.utils.sys_env import SysEnv


class GroupConsumer:

    def get_group_consumer(self, config):
        self.__redis_group_consumer(config)
        self.__redis_cluster_consumer(config)
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

    async def __redis_cluster_consumer(self, config):
        if config.type != MQConfig.REDIS_CLUSTER:
            return
        host = SysEnv.get("REDIS_HOST")
        port = int(SysEnv.get("REDIS_PORT"))
        from .aioredis_mq.group_consumer import GroupConsumer
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
        self.group_consumer = GroupConsumer(client=client, config=config)
        await self.group_consumer.create_group()
