from ..message import Message
from submodules.utils.logger import Logger

logger = Logger()


class GroupConsumer:

    def __init__(self, client, config):
        self.client = client
        self.config = config
        if self.config.block is None:
            self.config.block = 5
        if self.config.fromNowOn is None:
            self.config.fromNowOn = True
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = "0-0"
        if self.config.fromNowOn:
            self.lastId = ">"

    async def create_group(self):
        await self.client.xgroup_create(self.config.groupName)

    async def pull(self, count):
        async for message in self.client.xgroupread(
            self.config.groupName,
            self.config.consumerName,
            self.lastId,
            count,
            self.config.block
        ):
            yield Message(value=message)

    async def pendings(self):
        """当前已经被consumer读取了，但是没有ack的消息"""
        return await self.client.xpending(self.config.groupName)

    async def pending_range(self, count=None, consumerName=None):
        if count is None:
            count = 10
        if consumerName is None:
            consumerName = self.config.consumerName
        pending_info = await self.pendings()
        if pending_info.get("pending") == 0:
            return
        min_id = pending_info.get("min").decode()
        max_id = pending_info.get("max").decode()
        async for message in self.client.xpending_range(
                self.config.groupName, min_id, max_id, count, consumerName
        ):
            yield message

    async def claim(self, message_ids, min_idle_time=None):
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        return await self.client.claim(
            self.config.groupName,
            self.config.consumerName,
            min_idle_time,
            message_ids
        )

    async def autoclaim(self, min_idle_time=None, count=None):
        """
        将组中,已经pending了min_idle_time的消息声明到自己名下.

        min_idle_time: 空闲时间少于min_idle_time的消息将被过滤掉
        count: 一共声明多少条消息到自己名下
        """
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        if count is None:
            count = 10
        return await self.client.autoclaim(
            self.config.groupName,
            self.config.consumerName,
            min_idle_time,
            count
        )

    async def ack(self, message):
        value = message.value
        id = value[0].decode()
        logger.info(f"Redis GroupConsumer {self.config.groupName} {self.config.consumerName} ack {value}")
        await self.client.xack(self.config.groupName, id)
