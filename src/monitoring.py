import json
import threading
import time

from prometheus_client import Counter, Gauge, start_http_server

# Метрики Producer
producer_messages_total = Counter(
    'kafka_producer_messages_total',
    'Total number of messages sent',
    ['topic', 'status']
)
producer_message_size = Counter(
    'kafka_producer_message_size_bytes',
    'Size of messages sent in bytes',
    ['topic']
)
producer_batch_size = Gauge(
    'kafka_producer_batch_size',
    'Current batch size in messages',
    ['topic']
)
producer_latency = Gauge(
    'kafka_producer_latency_ms',
    'Producer latency in ms',
    ['topic']
)

# Метрики Consumer
consumer_messages_total = Counter(
    'kafka_consumer_messages_total',
    'Total number of messages consumed',
    ['topic', 'partition']
)
consumer_message_size = Counter(
    'kafka_consumer_message_size_bytes',
    'Size of messages consumed in bytes',
    ['topic']
)
consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Consumer lag in messages',
    ['topic', 'partition']
)
consumer_processing_time = Gauge(
    'kafka_consumer_processing_time_ms',
    'Message processing time in ms',
    ['topic']
)


# Запуск сервера метрик Prometheus
def start_metrics_server(port=8000):
    """Запускает HTTP-сервер для сбора метрик Prometheus."""
    start_http_server(port)
    print(f"Metrics server started on port {port}")


# Запуск в отдельном потоке
metrics_thread = threading.Thread(target=start_metrics_server)
metrics_thread.daemon = True
metrics_thread.start()


# Примеры использования в коде Producer
def send_with_metrics(producer, topic, value, key=None):
    start_time = time.time()
    size = len(json.dumps(value))

    # Пример установки текущего размера батча
    producer_batch_size.labels(topic=topic).set(producer._producer.flush(0))

    try:
        producer.send(topic, value, key)
        producer_messages_total.labels(topic=topic, status='success').inc()
    except Exception:
        producer_messages_total.labels(topic=topic, status='error').inc()
        raise

    producer_message_size.labels(topic=topic).inc(size)
    producer_latency.labels(topic=topic).set((time.time() - start_time) * 1000)


# Примеры использования в коде Consumer
def process_with_metrics(consumer, message):
    start_time = time.time()
    topic = message.topic()
    partition = message.partition()
    size = len(message.value())

    consumer_messages_total.labels(topic=topic, partition=partition).inc()
    consumer_message_size.labels(topic=topic).inc(size)

    # Обработка сообщения...

    consumer_processing_time.labels(topic=topic).set((time.time() - start_time) * 1000)

    # Обновление лага (требует дополнительных запросов к Kafka)
    # consumer_lag.labels(topic=topic, partition=partition).set(get_consumer_lag())
