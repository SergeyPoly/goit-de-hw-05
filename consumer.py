from kafka import KafkaConsumer, KafkaProducer
from configs import (
    kafka_config,
    building_sensors_topic_name,
    humidity_alerts_topic_name,
    temperature_alerts_topic_name,
)
import json
import uuid

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: v.decode("utf-8"),
    auto_offset_reset="earliest",  # Зчитування повідомлень з початку
    enable_auto_commit=True,  # Автоматичне підтвердження зчитаних повідомлень
    group_id="alerts_consumer_group",  # Ідентифікатор групи споживачів
)

# Створення Kafka Producer (для відправки відфільтрованих повідомлень)
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
)

consumer.subscribe([building_sensors_topic_name])
print(f"Subscribed to '{building_sensors_topic_name}'... Listening for messages.\n")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        sensor_data = message.value
        sensor_id = sensor_data["sensor_id"]
        temperature = sensor_data["temperature"]
        humidity = sensor_data["humidity"]
        timestamp = sensor_data["timestamp"]

        print(
            f"Received data from Sensor {sensor_id}: Temp={temperature}°C, Hum={humidity}%"
        )

        # Перевірка температури
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": f"WARNING! Temperature exceeded: {temperature}°C",
            }
            producer.send(
                temperature_alerts_topic_name, key=str(uuid.uuid4()), value=alert
            )
            print(f"Temp Alert Sent: {alert}")

        # Перевірка вологості
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": f"WARNING! Humidity out of range: {humidity}%",
            }
            producer.send(
                humidity_alerts_topic_name, key=str(uuid.uuid4()), value=alert
            )
            print(f"Humidity Alert Sent: {alert}")

        producer.flush()

except KeyboardInterrupt:
    print("Simulation stopped.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
