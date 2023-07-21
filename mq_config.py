class MQConfig:

    REDIS = "REDIS"
    KAFKA = "KAFKA"
    PULSAR = "PULSAR"

    def __init__(self, type):
        self.type = type

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return None

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __str__(self):
        return str(self.__dict__)
