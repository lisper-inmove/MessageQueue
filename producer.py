from .msg_config import MsgConfig


class Producer:

    def get_producer(self, msg_config):
        self.__redis_producer(msg_config)
        return self.producer

    def __redis_producer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        import redis
        from .redis_message_queue.producer import Producer
        from .redis_message_queue.client import Client

        client = Client(
            client=redis.StrictRedis(host=config.host, port=config.port),
            stream_name=config.stream_name
        )
        self.producer = Producer(client)
