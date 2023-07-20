from datetime import datetime


class Consumer:

    def __init__(self, client, fromNowOn=None, block=None):
        self.fromNowOn = fromNowOn
        if fromNowOn is None:
            self.fromNowOn = True
        self.block = block
        if block is None:
            self.block = 5
        self.client = client
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = '0-0'
        if self.fromNowOn:
            self.lastId = f"{int(datetime.now().timestamp() * 1000)}-0"

    async def pull(self, count):
        async for message in self.client.xread(count, self.lastId, self.block):
            self.lastId = message[0]
            yield message
