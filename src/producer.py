"""
Модуль надёжного производителя (producer) для Kafka.
Поддерживает автоматическое создание топиков, повторные попытки и сжатие.
"""
from confluent_kafka import Producer, KafkaException, admin
import json
import time
import socket
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s – %(name)s – %(levelname)s – %(message)s'
)
logger = logging.getLogger('kafka-producer')


class RobustProducer:
    """
    Надёжный продюсер для Kafka с автоматическими повторными попытками
    и возможностью автоматического создания топиков.
    """

    def __init__(self, bootstrap_servers, auto_create_topic=True):
        """
        Инициализация продюсера.

        :param bootstrap_servers: строка с адресом Kafka-брокера (например, 'localhost:9092')
        :param auto_create_topic: автоматически создавать топик, если он не существует
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'python-producer-{socket.gethostname()}',
            'retries': 5,
            'retry.backoff.ms': 100,
            'acks': 'all',
            'linger.ms': 5,
            'batch.size': 32768,
            'compression.type': 'snappy',  # Сжатие данных
        }
        self.auto_create_topic = auto_create_topic
        self._producer = None
        self._admin_client = None
        self._created_topics = set()  # Кеш созданных/проверенных топиков
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka и создаёт admin-клиента."""
        try:
            if self._producer is not None:
                self._producer.flush()
            self._producer = Producer(self.config)
            self._admin_client = admin.AdminClient(self.config)
            logger.info("Producer подключен к Kafka")
        except KafkaException as e:
            logger.error(f"Ошибка при создании Producer: {e}")
            raise

    def _ensure_topic_exists(self, topic):
        """
        Проверяет наличие топика и создаёт его при необходимости.

        :param topic: имя топика
        :return: True, если топик существует или создан; иначе False
        """
        if topic in self._created_topics:
            return True
        if not self.auto_create_topic:
            return False

        try:
            # Получаем метаданные о существующих топиках
            metadata = self._admin_client.list_topics(timeout=5)
            if topic in metadata.topics:
                self._created_topics.add(topic)
                return True

            # Топик не найден – создаём
            new_topic = admin.NewTopic(topic, num_partitions=1, replication_factor=1)
            fs = self._admin_client.create_topics([new_topic])
            fs[topic].result()  # Ждём завершения создания
            self._created_topics.add(topic)
            logger.info(f"Топик '{topic}' автоматически создан")
            return True
        except Exception as e:
            logger.error(f"Не удалось создать топик '{topic}': {e}")
            return False

    def _delivery_report(self, err, msg):
        """
        Колбэк для подтверждения доставки сообщения.
        Вызывается при успешной отправке или ошибке.
        """
        if err is not None:
            logger.error(
                f'Ошибка доставки: {err} для сообщения: {msg.value()[:30]}...'
            )
            # Здесь можно добавить сохранение сообщения для повторной отправки
        else:
            logger.debug(
                f'Сообщение доставлено в {msg.topic()}[{msg.partition()}] @ {msg.offset()}'
            )

    def send(self, topic, value, key=None, headers=None, max_retries=3):
        """
        Отправляет сообщение в Kafka с повторными попытками при ошибках.

        :param topic: топик Kafka
        :param value: значение (будет сериализовано в JSON)
        :param key: ключ сообщения (опционально)
        :param headers: заголовки (опционально)
        :param max_retries: максимальное количество повторных попыток
        :return: True, если сообщение успешно отправлено, иначе False
        """
        # Убедимся, что топик существует (если нужно)
        if self.auto_create_topic and not self._ensure_topic_exists(topic):
            logger.error(f"Топик '{topic}' не доступен и не может быть создан")
            return False

        retry_count = 0
        while retry_count <= max_retries:
            try:
                # Сериализация данных
                value_bytes = json.dumps(value, ensure_ascii=False).encode('utf-8')
                key_bytes = str(key).encode('utf-8') if key else None

                # Отправка сообщения
                self._producer.produce(
                    topic=topic,
                    value=value_bytes,
                    key=key_bytes,
                    headers=headers,
                    callback=self._delivery_report
                )

                # Периодически вызываем poll для обработки колбэков
                self._producer.poll(0)
                return True

            except BufferError:
                # Буфер сообщений переполнен, ждём и пробуем снова
                logger.warning("Буфер Producer переполнен, ожидание...")
                self._producer.poll(1)
                retry_count += 1

            except KafkaException as e:
                logger.error(f"Ошибка Kafka при отправке: {e}")
                retry_count += 1
                if retry_count <= max_retries:
                    # Экспоненциальная задержка перед повторной попыткой
                    time.sleep(0.5 * retry_count)
                    try:
                        self._connect()  # Пробуем переподключиться
                    except Exception as conn_err:
                        logger.error(f"Не удалось переподключиться: {conn_err}")
                # Если лимит попыток исчерпан, цикл завершится и вернётся False

            # Небольшая задержка перед следующей попыткой в случае ошибки
            time.sleep(0.1)

        # Все попытки исчерпаны
        logger.error(f"Не удалось отправить сообщение после {max_retries + 1} попыток")
        return False

    def flush(self, timeout=None):
        """Сбрасывает буфер продюсера."""
        if self._producer:
            return self._producer.flush(timeout)

    def close(self):
        """Закрывает продюсер (освобождает ресурсы)."""
        if self._producer:
            self._producer.flush()
            # В confluent-kafka нет явного закрытия, продюсер закроется при сборке мусора
            self._producer = None
