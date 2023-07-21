class GroupConsumer:

    def __init__(self, client, config):
        self.client = client
        if self.config.block is None:
            self.config.block = 5
        if self.config.fromNowOn is None:
            self.config.fromNowOn = True
        self.client.xgroup_create(self.config.groupName)
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = "0-0"
        if self.fromNowOn:
            self.lastId = ">"

    def pull(self, count):
        messages = self.client.xgroupread(
            self.config.groupName,
            self.config.consumerName,
            self.lastId,
            count,
            self.config.block
        )
        if len(messages) > 0:
            self.lastId = messages[-1][0]
        return messages

    def pendings(self):
        """当前已经被consumer读取了，但是没有ack的消息"""
        return self.client.xpending(self.config.groupName)

    def pending_range(self, count=None, consumerName=None):
        if count is None:
            count = 10
        if consumerName is None:
            consumerName = self.config.consumerName
        pending_info = self.pendings()
        if pending_info.get("pending") == 0:
            return []
        minId = pending_info.get("min").decode()
        maxId = pending_info.get("max").decode()
        return self.client.xpending_range(self.config.groupName, minId, maxId, count, consumerName)

    def claim(self, message_ids, min_idle_time=None):
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        return self.client.claim(self.config.groupName, self.config.consumerName, min_idle_time, message_ids)

    def autoclaim(self, min_idle_time=None, count=None):
        """
        将组中,已经pending了min_idle_time的消息声明到自己名下.

        min_idle_time: 空闲时间少于min_idle_time的消息将被过滤掉
        count: 一共声明多少条消息到自己名下
        """
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        if count is None:
            count = 10
        return self.client.autoclaim(self.config.groupName, self.config.consumerName, min_idle_time, count)

    def ack(self, id):
        self.client.xack(self.config.groupName, id)
