from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

p.produce('mytopic', 'Hello, Kafka', callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports
# to be received.
p.flush()
