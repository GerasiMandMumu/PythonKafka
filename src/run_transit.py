from producer import RobustProducer
from consumer import RobustConsumer
import threading
import time


def consumer_thread():
    def process(data, msg):
        print(f"Consumer получил: {data}")

    cons = RobustConsumer('localhost:9092', 'test-group', 'test-topic')
    cons.process_batch(process, batch_size=5, timeout=0.5)


def producer_thread():
    prod = RobustProducer('localhost:9092')
    for i in range(5):
        prod.send('test-topic', {'num': i, 'msg': f'hello {i}'})
        time.sleep(0.2)
    prod.close()


if __name__ == "__main__":
    # Запускаем consumer в фоне
    t = threading.Thread(target=consumer_thread)
    t.daemon = True
    t.start()
    time.sleep(1)  # даём consumer время подключиться
    producer_thread()
    time.sleep(2)  # ждём обработки
