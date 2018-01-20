import os

from kafka import KafkaProducer

source = os.path.join(os.path.dirname(__file__), '..', 'PS_20174392719_1491204439457_log_10k.csv')
producer = KafkaProducer(bootstrap_servers='172.17.0.1:9092')


with open(source) as sfile:
    for line in sfile:
        producer.send('test', line.encode(encoding='utf-8'))
