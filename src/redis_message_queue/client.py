import redis
from redis import StrictRedis
from submodules.utils.logger import Logger

logger = Logger()


class Client:

    def __init__(self, client, stream_name, maxlen=None):
        if maxlen is None:
            maxlen = 1000
        self.maxlen = maxlen
        self.client = client
        # 默认只获取最新数据
        self.from_now_on = True
        self.stream_name = stream_name

    def xgroup_create(self, group_name, mkstream=None):
        if mkstream is None:
            mkstream = True
        try:
            self.client.xgroup_create(
                self.stream_name,
                group_name,
                mkstream=mkstream
            )
        except redis.exceptions.ResponseError as ex:
            if str(ex) == "BUSYGROUP Consumer Group name already exists":
                return
            raise ex

    def xread(self, count, last_id, block):
        logger.info(f"{self.stream_name} xread: {count} {last_id} {block}")
        messages = self.client.xread(
            streams={self.stream_name: last_id},
            count=count,
            block=block,
        )
        if len(messages) == 0:
            return []
        messages = messages[0][1]
        return messages

    def xgroupread(self, group_name, consumer_name, last_id, count, block):
        logger.info(f"{self.stream_name} xgroupread: {group_name} {consumer_name} {last_id} {count} {block}")
        messages = self.client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={self.stream_name: last_id},
            count=count,
            block=block)
        if len(messages) == 0:
            return []
        messages = messages[0][1]
        return messages

    def xadd(self, data):
        logger.info(f"{self.stream_name} xadd: {data} {self.maxlen}")
        result = self.client.xadd(
            self.stream_name,
            data,
            maxlen=self.maxlen,
            approximate=True
        )
        return result

    def xdel(self, id):
        logger.info(f"{self.stream_name} xdel: {id}")
        self.client.xdel(self.stream_name, id)

    def xack(self, group_name, id):
        logger.info(f"{self.stream_name} xack: {group_name} {id}")
        return self.client.xack(self.stream_name, group_name, id)

    def xpending(self, group_name):
        logger.info(f"{self.stream_name} xpending: {group_name}")
        return self.client.xpending(self.stream_name, group_name)

    def xpending_range(self, group_name, min_id, max_id, count, consumer_name=None, idle=None):
        if idle is None:
            idle = 10 * 1000
        logger.info(f"{self.stream_name} xpending_range: {group_name}")
        return self.client.xpending_range(
            self.stream_name, group_name, min_id, max_id, count, consumer_name, idle)

    def claim(self, group_name, consumer_name, min_idle_time, message_ids):
        logger.info(f"{self.stream_name} claim: {group_name} {consumer_name} {min_idle_time} {message_ids}")
        return self.client.xclaim(self.stream_name, group_name, consumer_name, min_idle_time, message_ids)

    def autoclaim(self, group_name, consumer_name, min_idle_time, count, start_id=None):
        """
        关于 xautoclaim的返回值: https://redis.io/commands/xautoclaim/
        长度为3的数组
        1. 下一次start_id传的值
        2. 当前声明成功的消息列表
        3. stream中已经不存在的消息列表
        """
        if start_id is None:
            start_id = "0-0"
        logger.info(f"{self.stream_name} autoclaim: {group_name} {consumer_name} {min_idle_time} {count}")
        messages = self.client.xautoclaim(
            self.stream_name,
            group_name,
            consumer_name,
            min_idle_time,
            count=count,
            start_id=start_id
        )
        return messages
