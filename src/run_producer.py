"""
Пример запуска продюсера для отправки сообщений в Kafka.
"""
import os
import time
from producer import RobustProducer

# Адрес Kafka можно задать через переменную окружения
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'test-topic'


def main():
    # Создаём продюсер с автоматическим созданием топика
    producer = RobustProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_create_topic=True
    )

    countries = [
        {
            "name": "Страна1"
        },
        {
            "name": "Страна2"
        }]

    try:
        for i in range(len(countries)):
            message = countries[i]
            success = producer.send(
                topic=TOPIC,
                value=message,
                key=str(i)  # необязательный ключ
            )
            if success:
                print(f"Сообщение {i} отправлено")
            else:
                print(f"Ошибка при отправке {i}")
            time.sleep(0.5)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
