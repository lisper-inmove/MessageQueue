import time

from confluent_kafka.admin import AdminClient, NewTopic


class Client:

    def create_topic(self, config):
        admin_client = AdminClient({
            "bootstrap.servers": config.bootstrap_servers
        })
        metadata = admin_client.list_topics(
            timeout=5
        )
        while config.topic not in metadata.topics:
            print(metadata.topics)
            new_topic = NewTopic(
                config.topic,
                num_partitions=1,
                replication_factor=1
            )
            res = admin_client.create_topics(
                [new_topic],
                validate_only=False,
                operation_timeout=100
            )
