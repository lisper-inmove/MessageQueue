class Producer:

    def __init__(self, client):
        self.client = client

    def push(self, data):
        self.client.xadd(data)
