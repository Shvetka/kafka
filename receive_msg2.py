import json
from confluent_kafka import Consumer, KafkaError, TopicPartition


# Настройка консьюмера – адрес сервера
conf = {
    "bootstrap.servers": "project-kafka-0-1:9092",
    "group.id": "my-group2",
    "acks": "all",
    "fetch.min.bytes": "2000",
    "fetch.max.wait.ms": "1000"
    "auto.offset.reset": "earliest",
    "retries": 5
}
# Создание консьюмера
consumer = Consumer(conf)

# Подписка на топик
consumer.subscribe(["sensor_data"])

# Чтение сообщений в бесконечном цикле
try:
    while True:
        msgs = consumer.consume(num_messages=10, timeout=1.0)
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(msg.error())
            else:
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                consumer.commit(asynchronous=False)
finally:
    # Закрытие консьюмера
    consumer.close() 
