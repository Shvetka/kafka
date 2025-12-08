import json
from confluent_kafka import Consumer


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
        # Получение сообщений
        msg = consumer.poll(0.2)

        if msg is None:
            continue
        if msg.error():
            print(f"Ошибка: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        consumer.commit(asynchronous=False)
        print(f"Получено сообщение: {value=}, offset={msg.offset()}")
finally:
    # Закрытие консьюмера
    consumer.close() 
