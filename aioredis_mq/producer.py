class Producer:

    def __init__(self, client, config):
        self.client = client
        self.config = config

    async def push(self, data):
        await self.client.xadd(data)

    async def cleanup(self):
        self.client = None
