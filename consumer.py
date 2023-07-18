from msg_config import MsgConfig


class Consumer:

    def get_consumer(self, msg_config):
        self.__redis_consumer(msg_config)
        return self.producer

    def __redis_consumer(self, config):
        if config.type != MsgConfig.REDIS:
            return
        import redis
        from redis_message_queue.consumer import Consumer
        from redis_message_queue.client import Client

        client = Client(
            client=redis.StrictRedis(host=config.host, port=config.port),
            stream_name=config.stream_name
        )
        self.producer = Consumer(client)
