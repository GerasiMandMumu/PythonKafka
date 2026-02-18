from confluent_kafka.admin import AdminClient

admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
metadata = admin_client.list_topics(timeout=10)

print("Доступные топики Kafka:")
for topic, topic_metadata in metadata.topics.items():
    print(f" – {topic}")
