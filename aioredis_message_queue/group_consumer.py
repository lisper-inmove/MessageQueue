class GroupConsumer:

    def __init__(self, client, group_name, consumer_name,
                 from_now_on=None, block=None):
        self.client = client
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.block = block
        if self.block is None:
            self.block = 5
        self.from_now_on = from_now_on
        if self.from_now_on is None:
            self.from_now_on = True
        self.__set_last_id()

    def __set_last_id(self):
        self.last_id = "0-0"
        if self.from_now_on:
            self.last_id = ">"

    async def create_group(self):
        await self.client.xgroup_create(self.group_name)

    async def pull(self, count):
        async for message in self.client.xgroupread(
            self.group_name,
            self.consumer_name,
            self.last_id,
            count,
            self.block
        ):
            yield message

    async def pendings(self):
        """当前已经被consumer读取了，但是没有ack的消息"""
        return await self.client.xpending(self.group_name)

    async def pending_range(self, count=None, consumer_name=None):
        if count is None:
            count = 10
        if consumer_name is None:
            consumer_name = self.consumer_name
        pending_info = await self.pendings()
        if pending_info.get("pending") == 0:
            return
        min_id = pending_info.get("min").decode()
        max_id = pending_info.get("max").decode()
        async for message in self.client.xpending_range(
                self.group_name, min_id, max_id, count, consumer_name
        ):
            yield message

    async def claim(self, message_ids, min_idle_time=None):
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        return await self.client.claim(self.group_name, self.consumer_name, min_idle_time, message_ids)

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
        return await self.client.autoclaim(self.group_name, self.consumer_name, min_idle_time, count)

    async def ack(self, id):
        await self.client.xack(self.group_name, id)
