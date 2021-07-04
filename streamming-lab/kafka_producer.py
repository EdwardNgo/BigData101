from kafka import KafkaProducer
import json
import time
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def produce(topic,message):
    producer.send(topic,message)
    producer.flush()

if __name__ == '__main__':
    with open('log.txt') as f:
        data = f.read()
    for line in data.split("\n"):
        producer.send('sample',line)
        time.sleep(1)
