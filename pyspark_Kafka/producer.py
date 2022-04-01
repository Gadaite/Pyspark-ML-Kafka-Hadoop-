from kafka import KafkaProducer
from kafka.errors import KafkaError
producer =  KafkaProducer(bootstrap_servers='192.168.1.10:9092')
while True:
    msg = input("input the msg:")
    future = producer.send("sparkapp", msg.encode())
    # def send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
    try:
        record = future.get(timeout=10)
        # print(record)
    except KafkaError as e:
        print(e)