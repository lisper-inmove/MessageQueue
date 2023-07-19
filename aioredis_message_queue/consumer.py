from datetime import datetime


class Consumer:

    def __init__(self, client, from_now_on=None, block=None):
        self.from_now_on = from_now_on
        if from_now_on is None:
            self.from_now_on = True
        self.block = block
        if block is None:
            self.block = 5
        self.client = client
        self.__set_last_id()

    def __set_last_id(self):
        self.last_id = '0-0'
        if self.from_now_on:
            self.last_id = f"{int(datetime.now().timestamp() * 1000)}-0"

    async def pull(self, count):
        async for message in self.client.xread(count, self.last_id, self.block):
            self.last_id = message[0]
            yield message
