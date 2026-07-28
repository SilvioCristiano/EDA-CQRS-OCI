"""Cria tópicos explicitamente no OCI Streaming with Apache Kafka."""
import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

topic = os.environ["KAFKA_TOPIC"]
partitions = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "6"))
replication_factor = int(os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "3"))
admin = KafkaAdminClient(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
    security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_AUTH_TOKEN"],
)
try:
    admin.create_topics(new_topics=[
        NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor),
        NewTopic(name=f"{topic}.dlq", num_partitions=partitions, replication_factor=replication_factor),
    ], validate_only=False)
except TopicAlreadyExistsError:
    pass
finally:
    admin.close()
