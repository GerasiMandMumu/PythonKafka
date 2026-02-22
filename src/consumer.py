"""
Модуль надёжного потребителя (consumer) для Kafka.
Поддерживает батчевую обработку, автоматическое переподключение и создание топиков.
"""
from confluent_kafka import Consumer, KafkaError, KafkaException, admin
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s – %(name)s – %(levelname)s – %(message)s'
)
logger = logging.getLogger('kafka-consumer')


class RobustConsumer:
    """
    Надёжный потребитель для Kafka с обработкой батчей,
    автоматическим переподключением и проверкой существования топиков.
    """

    def __init__(self, bootstrap_servers, group_id, topics, error_handler=None,
                 auto_create_topic=True, unique_group_suffix=False):
        """
        Инициализация потребителя.

        :param bootstrap_servers: адрес Kafka-брокера (строка)
        :param group_id: идентификатор группы потребителей
        :param topics: топик или список топиков для подписки
        :param error_handler: опциональная функция для обработки ошибок (принимает строку)
        :param auto_create_topic: автоматически создавать топики, если они не существуют
        :param unique_group_suffix: добавлять случайный суффикс к group.id (для изоляции тестовых запусков)
        """
        import uuid

        self.base_group_id = group_id
        if unique_group_suffix:
            group_id = f"{group_id}-{uuid.uuid4().hex[:8]}"
            logger.info(f"Используется уникальный group.id: {group_id}")

        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 600000,        # 10 минут
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.partition.fetch.bytes': 1048576,  # 1MB
        }
        self.topics = topics if isinstance(topics, list) else [topics]
        self.error_handler = error_handler
        self.auto_create_topic = auto_create_topic
        self._consumer = None
        self._admin_client = None
        self._running = False

    def _ensure_topics_exist(self):
        """Проверяет наличие всех топиков и создаёт отсутствующие."""
        if not self.auto_create_topic:
            return True
        try:
            self._admin_client = admin.AdminClient(self.config)
            metadata = self._admin_client.list_topics(timeout=5)
            existing = set(metadata.topics.keys())
            to_create = [t for t in self.topics if t not in existing]
            if to_create:
                new_topics = [admin.NewTopic(t, num_partitions=1, replication_factor=1) for t in to_create]
                fs = self._admin_client.create_topics(new_topics)
                for topic, f in fs.items():
                    f.result()  # ждём создания каждого
                    logger.info(f"Топик '{topic}' автоматически создан")
            return True
        except Exception as e:
            logger.error(f"Ошибка при проверке/создании топиков: {e}")
            return False

    def _connect(self):
        """Устанавливает соединение с Kafka и подписывается на топики."""
        try:
            if self._consumer is not None:
                self._consumer.close()
            self._consumer = Consumer(self.config)
            # Проверяем/создаём топики перед подпиской
            self._ensure_topics_exist()
            self._consumer.subscribe(self.topics)
            logger.info(f"Consumer подключен к Kafka и подписан на топики: {self.topics}")
            return True
        except KafkaException as e:
            logger.error(f"Ошибка при создании Consumer: {e}")
            return False

    def process_batch(self, process_message_func, batch_size=100, timeout=1.0):
        """
        Обрабатывает батч сообщений с помощью переданной функции.
        Работает до вызова close().

        :param process_message_func: функция, принимающая (data, message)
        :param batch_size: максимальное количество сообщений в батче
        :param timeout: таймаут ожидания сообщений (сек)
        """
        if not self._connect():
            return

        self._running = True

        try:
            while self._running:
                try:
                    messages = self._consumer.consume(batch_size, timeout)
                    if not messages:
                        continue

                    processed_offsets = {}  # Отслеживаем обработанные смещения по партициям

                    for msg in messages:
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(f"Достигнут конец партиции {msg.partition()}")
                            else:
                                error_msg = f"Ошибка при получении сообщения: {msg.error()}"
                                logger.error(error_msg)
                                if self.error_handler:
                                    self.error_handler(error_msg)
                        else:
                            try:
                                # Декодируем и парсим JSON
                                value_str = msg.value().decode('utf-8')
                                data = json.loads(value_str)
                                process_message_func(data, msg)

                                # Запоминаем позицию для коммита
                                key = f"{msg.topic()}-{msg.partition()}"
                                processed_offsets[key] = msg

                            except json.JSONDecodeError:
                                logger.error(f"Ошибка декодирования JSON: {value_str[:100]}")
                            except Exception as e:
                                logger.error(f"Ошибка обработки сообщения: {e}")
                                if self.error_handler:
                                    self.error_handler(f"Ошибка обработки сообщения: {e}")

                    # Коммитим обработанные смещения
                    if processed_offsets:
                        try:
                            for msg in processed_offsets.values():
                                self._consumer.commit(message=msg, asynchronous=False)
                        except KafkaException as e:
                            logger.error(f"Ошибка при коммите смещений: {e}")

                except KafkaException as e:
                    logger.error(f"Ошибка Kafka: {e}")
                    # Пробуем переподключиться
                    time.sleep(1)
                    self._connect()

        finally:
            self.close()

    def close(self):
        """Останавливает потребителя и освобождает ресурсы."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None