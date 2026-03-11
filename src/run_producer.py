"""
Интерактивный продюсер для Kafka.
Отправляет сообщения по одному при вводе пользователя.
"""
import os
import sys
from producer import RobustProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'test-topic'  # можно изменить при необходимости


def main():
    # Создаём продюсер с автоматическим созданием топика
    producer = RobustProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_create_topic=True
    )

    print("Интерактивный продюсер Kafka. Введите название страны для отправки сообщения.")
    print(f"Топик: {TOPIC}")
    print("Для выхода введите 'exit' или нажмите Ctrl+C\n")

    try:
        while True:
            user_input = input("Страна > ").strip()
            if user_input.lower() == 'exit':
                print("Завершение работы.")
                break
            if not user_input:
                print("Пустой ввод, попробуйте снова.")
                continue

            message = {"name": user_input}
            success = producer.send(
                topic=TOPIC,
                value=message,
                key=None  # ключ не обязателен
            )
            if success:
                print(f"✅ Сообщение '{user_input}' отправлено в топик {TOPIC}")
            else:
                print(f"❌ Ошибка при отправке сообщения '{user_input}'")
    except KeyboardInterrupt:
        print("\nПрерывание по Ctrl+C.")
    finally:
        producer.close()
        print("Продюсер закрыт.")


if __name__ == "__main__":
    main()
