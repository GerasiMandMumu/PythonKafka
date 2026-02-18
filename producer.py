from confluent_kafka import Producer, KafkaException
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
    """Надёжный продюсер для Kafka с автоматическими повторными попытками."""

    def __init__(self, bootstrap_servers):
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
        self._producer = None
        self._connect()

    def _connect(self):
        """Устанавливает соединение с Kafka."""
        try:
            if self._producer is not None:
                self._producer.flush()
            self._producer = Producer(self.config)
            logger.info("Producer подключен к Kafka")
        except KafkaException as e:
            logger.error(f"Ошибка при создании Producer: {e}")
            raise

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
        retry_count = 0
        while retry_count <= max_retries:
            try:
                # Сериализация данных
                value_bytes = json.dumps(value).encode('utf-8')
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
                        # Если переподключение не удалось, продолжаем попытки
                        # (оставляем текущий продюсер, он может восстановиться сам)
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
