from .msg_config import MsgConfig


class GroupConsumer:

    def get_producer(self, msg_config):
        self.__redis_producer(msg_config)
        return self.producer

    def __redis_producer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        import redis
        from .redis_message_queue.group_consumer import GroupConsumer
        from .redis_message_queue.client import Client

        client = Client(
            client=redis.StrictRedis(host=config.host, port=config.port),
            stream_name=config.stream_name
        )
        self.producer = GroupConsumer(client, config.group_name, config.consumer_name)
