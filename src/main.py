#!/usr/bin/env python3
"""
Скрипт для просмотра доступных топиков в Kafka.
"""
import os
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def list_topics():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    try:
        metadata = admin_client.list_topics(timeout=10)
        print("Доступные топики Kafka:")
        for topic in metadata.topics.keys():
            print(f" – {topic}")
    except Exception as e:
        print(f"Ошибка подключения к Kafka: {e}")

if __name__ == "__main__":
    list_topics()