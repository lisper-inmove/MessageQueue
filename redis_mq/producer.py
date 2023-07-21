class Producer:

    def __init__(self, client, config):
        self.client = client
        self.config = config

    def push(self, data):
        self.client.xadd(data)

    def cleanup(self):
        pass
