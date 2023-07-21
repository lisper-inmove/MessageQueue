from datetime import datetime


class Consumer:

    def __init__(self, client, config):
        self.config = config
        if self.config.fromNowOn is None:
            self.config.fromNowOn = True
        if self.config.block is None:
            self.config.block = 5
        self.client = client
        self.__set_last_id()

    def __set_last_id(self):
        self.lastId = '0-0'
        if self.config.fromNowOn:
            self.lastId = f"{int(datetime.now().timestamp() * 1000)}-0"

    def pull(self, count):
        messages = self.client.xread(count, self.lastId, self.config.block)
        if len(messages) > 0:
            self.lastId = messages[-1][0]
        return messages
