from datetime import datetime
from ..message import Message


class Consumer:

    def __init__(self, client, config):
        self.config = config
        if config.fromNowOn is None:
            config.fromNowOn = True
        if config.block is None:
            config.block = 5
        self.client = client
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = '0-0'
        if self.config.fromNowOn:
            self.lastId = f"{int(datetime.now().timestamp() * 1000)}-0"

    async def pull(self, count):
        async for message in self.client.xread(
                count,
                self.lastId,
                self.config.block):
            self.lastId = message[0]
            yield Message(
                value=message
            )
