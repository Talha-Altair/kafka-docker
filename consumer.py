from confluent_kafka import Consumer, KafkaError
import json

c = Consumer({
    'bootstrap.servers': 'localhost:9091',
    'group.id': 'altair-consumers',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['project-x'])

def startpy():

    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print(json.loads(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print("End of partition reached")
        else:
            print("Error")

if __name__ == '__main__':
    startpy()