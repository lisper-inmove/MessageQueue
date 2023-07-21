import redis


class Client:

    def __init__(self, client, config):
        self.config = config
        if self.config.maxlen is None:
            self.config.maxlen = 1000
        if self.config.fromNowOn is None:
            self.config.fromNowOn = True
        self.client = client

    async def xgroup_create(self, groupName, mkstream=None):
        if mkstream is None:
            mkstream = True
        try:
            await self.client.xgroup_create(
                self.config.topic,
                groupName,
                mkstream=mkstream
            )
        except redis.exceptions.ResponseError as ex:
            if str(ex) == "BUSYGROUP Consumer Group name already exists":
                return
            raise ex

    async def xread(self, count, lastId, block):
        messages = await self.client.xread(
            streams={self.config.topic: lastId},
            count=count,
            block=block
        )
        if len(messages) == 0:
            return
        for message in messages[0][1]:
            yield message

    async def xgroupread(self, groupName, consumerName, lastId, count, block):
        messages = await self.client.xreadgroup(
            groupname=groupName,
            consumername=consumerName,
            streams={self.config.topic: lastId},
            count=count,
            block=block
        )
        if len(messages) == 0:
            return
        for message in messages[0][1]:
            yield message

    async def xadd(self, data):
        return await self.client.xadd(
            self.config.topic,
            data,
            maxlen=self.config.maxlen,
            approximate=True
        )

    async def xdel(self, id):
        await self.client.xdel(self.config.topic, id)

    async def xack(self, groupName, id):
        return await self.client.xack(self.config.topic, groupName, id)

    async def xpending(self, groupName):
        return await self.client.xpending(self.config.topic, groupName)

    async def xpending_range(self, groupName, min_id, max_id, count, consumerName=None, idle=None):
        if idle is None:
            idle = 10 * 1000
        messages = await self.client.xpending_range(
            self.config.topic,
            groupName,
            min_id,
            max_id,
            count,
            consumerName,
            idle
        )
        for message in messages:
            yield message

    async def claim(self, groupName, consumerName, min_idle_time, message_ids):
        return await self.client.xclaim(
            self.config.topic,
            groupName,
            consumerName,
            min_idle_time,
            message_ids
        )

    async def autoclaim(self, groupName, consumerName, min_idle_time, count, start_id=None):
        """
        关于 xautoclaim的返回值: https://redis.io/commands/xautoclaim/
        长度为3的数组
        1. 下一次start_id传的值
        2. 当前声明成功的消息列表
        3. stream中已经不存在的消息列表
        """
        if start_id is None:
            start_id = "0-0"
        messages = await self.client.xautoclaim(
            self.config.topic,
            groupName,
            consumerName,
            min_idle_time,
            count=count,
            start_id=start_id
        )
        for message in messages:
            yield message
