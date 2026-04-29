from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])


try:
    topic = sys.argv[1]
    processed_topic = sys.argv[2]
except:
    print('Usage: python3 consumer.py <topic> <processed_topic>')
    exit(1)

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic])

print(f'Consuming from "{topic}" and forwarding to "{processed_topic}"...')

for msg in consumer:
    print(msg.value)
    producer.send(processed_topic, value=msg.encode())

producer.flush()
