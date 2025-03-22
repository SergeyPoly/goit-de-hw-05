from kafka.admin import KafkaAdminClient, NewTopic
from configs import (
    kafka_config,
    building_sensors_topic_name,
    humidity_alerts_topic_name,
    temperature_alerts_topic_name,
    my_name,
)

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

num_partitions = 2
replication_factor = 1

topics = [
    NewTopic(
        name=building_sensors_topic_name,
        num_partitions=num_partitions,
        replication_factor=1,
    ),
    NewTopic(
        name=temperature_alerts_topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    ),
    NewTopic(
        name=humidity_alerts_topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    ),
]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список створених топіків
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

# Закриття зв'язку з клієнтом
admin_client.close()
