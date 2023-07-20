class GroupConsumer:

    def __init__(self, client, groupName, consumerName,
                 fromNowOn=None, block=None):
        self.client = client
        self.groupName = groupName
        self.consumerName = consumerName
        self.block = block
        if self.block is None:
            self.block = 5
        self.fromNowOn = fromNowOn
        if self.fromNowOn is None:
            self.fromNowOn = True
        self.client.xgroup_create(groupName)
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = "0-0"
        if self.fromNowOn:
            self.lastId = ">"

    def pull(self, count):
        messages = self.client.xgroupread(
            self.groupName,
            self.consumerName,
            self.lastId,
            count,
            self.block
        )
        if len(messages) > 0:
            self.lastId = messages[-1][0]
        return messages

    def pendings(self):
        """当前已经被consumer读取了，但是没有ack的消息"""
        return self.client.xpending(self.groupName)

    def pending_range(self, count=None, consumerName=None):
        if count is None:
            count = 10
        if consumerName is None:
            consumerName = self.consumerName
        pending_info = self.pendings()
        if pending_info.get("pending") == 0:
            return []
        min_id = pending_info.get("min").decode()
        max_id = pending_info.get("max").decode()
        return self.client.xpending_range(self.groupName, min_id, max_id, count, consumerName)

    def claim(self, message_ids, min_idle_time=None):
        if min_idle_time is None:
            min_idle_time = 10 * 1000
        return self.client.claim(self.groupName, self.consumerName, min_idle_time, message_ids)

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
        return self.client.autoclaim(self.groupName, self.consumerName, min_idle_time, count)

    def ack(self, id):
        self.client.xack(self.groupName, id)
