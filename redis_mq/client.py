import redis


class Client:

    def __init__(self, client, config):
        self.config = config
        if self.config.maxlen is None:
            self.config.maxlen = 1000
        self.client = client

    def xgroup_create(self, groupName, mkstream=None):
        if mkstream is None:
            mkstream = True
        try:
            self.client.xgroup_create(
                self.config.streamName,
                groupName,
                mkstream=mkstream
            )
        except redis.exceptions.ResponseError as ex:
            if str(ex) == "BUSYGROUP Consumer Group name already exists":
                return
            raise ex

    def xread(self, count, lastId, block):
        messages = self.client.xread(
            streams={self.config.streamName: lastId},
            count=count,
            block=block,
        )
        if len(messages) == 0:
            return []
        messages = messages[0][1]
        return messages

    def xgroupread(self, groupName, consumerName, lastId, count, block):
        messages = self.client.xreadgroup(
            groupname=groupName,
            consumername=consumerName,
            streams={self.config.streamName: lastId},
            count=count,
            block=block)
        if len(messages) == 0:
            return []
        messages = messages[0][1]
        return messages

    def xadd(self, data):
        result = self.client.xadd(
            self.config.streamName,
            data,
            maxlen=self.config.maxlen,
            approximate=True
        )
        return result

    def xdel(self, id):
        self.client.xdel(self.config.streamName, id)

    def xack(self, groupName, id):
        return self.client.xack(self.config.streamName, groupName, id)

    def xpending(self, groupName):
        return self.client.xpending(self.config.streamName, groupName)

    def xpending_range(self, groupName, min_id, max_id, count, consumerName=None, idle=None):
        if idle is None:
            idle = 10 * 1000
        return self.client.xpending_range(
            self.config.streamName, groupName, min_id, max_id, count, consumerName, idle)

    def claim(self, groupName, consumerName, min_idle_time, message_ids):
        return self.client.xclaim(self.config.streamName, groupName, consumerName, min_idle_time, message_ids)

    def autoclaim(self, groupName, consumerName, min_idle_time, count, start_id=None):
        """
        关于 xautoclaim的返回值: https://redis.io/commands/xautoclaim/
        长度为3的数组
        1. 下一次start_id传的值
        2. 当前声明成功的消息列表
        3. stream中已经不存在的消息列表
        """
        if start_id is None:
            start_id = "0-0"
        messages = self.client.xautoclaim(
            self.config.streamName,
            groupName,
            consumerName,
            min_idle_time,
            count=count,
            start_id=start_id
        )
        return messages
