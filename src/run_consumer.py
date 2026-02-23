"""
Пример запуска потребителя для чтения сообщений из Kafka.
"""
import os
import logging
from consumer import RobustConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'test-topic'
GROUP_ID = 'my-group'


def process_message(data, msg):
    """
    Функция обработки одного сообщения.
    Здесь можно реализовать любую логику.
    """
    print(f"Получено сообщение: {data}")
    logger = logging.getLogger('processor')
    logger.info(f"Обработано: ключ={msg.key()}, смещение={msg.offset()}")


def main():
    # Создаём потребитель с автоматическим созданием топика
    # и уникальным group.id для изоляции тестового запуска
    consumer = RobustConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        topics=TOPIC,
        auto_create_topic=True,
        unique_group_suffix=True  # добавляет случайный суффикс
    )

    print("Consumer запущен, ожидание сообщений...")
    # Запускаем бесконечную обработку батчей
    consumer.process_batch(
        process_message_func=process_message,
        batch_size=10,
        timeout=1.0
    )


if __name__ == "__main__":
    main()
