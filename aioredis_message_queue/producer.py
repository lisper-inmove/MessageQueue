class Producer:

    def __init__(self, client):
        self.client = client

    async def push(self, data):
        await self.client.xadd(data)
