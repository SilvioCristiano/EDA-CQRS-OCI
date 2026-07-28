"""Consumidor Kafka para a projeção CQRS; execute como Deployment no OKE.

O commit é feito somente após o upsert. A tabela de deduplicação torna o
reprocessamento seguro. Configure credenciais via Secret do Vault/CSI driver.
"""
import json
import os

from kafka import KafkaConsumer
import oracledb


def apply_event(conn, event):
    payload = event["payload"]
    with conn.cursor() as cur:
        # MERGE só avança a projeção se a versão recebida for mais nova.
        cur.execute("SELECT 1 FROM processed_events WHERE event_id = :1", [event["eventId"]])
        if cur.fetchone():
            return
        cur.execute(
            """MERGE INTO order_read_model d USING (SELECT :1 order_id, :2 customer_id,
                :3 version, :4 payload FROM dual) s ON (d.order_id=s.order_id)
                WHEN MATCHED THEN UPDATE SET d.customer_id=s.customer_id, d.version=s.version,
                d.payload=s.payload, d.updated_at=SYSTIMESTAMP WHERE d.version < s.version
                WHEN NOT MATCHED THEN INSERT (order_id, customer_id, version, payload, updated_at)
                VALUES (s.order_id,s.customer_id,s.version,s.payload,SYSTIMESTAMP)""",
            [event["aggregateId"], payload["customerId"], event["aggregateVersion"], json.dumps(payload)],
        )
        cur.execute("INSERT INTO processed_events(event_id, processed_at) VALUES (:1,SYSTIMESTAMP)", [event["eventId"]])
    conn.commit()


consumer = KafkaConsumer(
    os.environ["KAFKA_TOPIC"], bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
    group_id=os.environ.get("KAFKA_GROUP_ID", "orders-read-model-v1"), enable_auto_commit=False,
    security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ["KAFKA_USERNAME"], sasl_plain_password=os.environ["KAFKA_AUTH_TOKEN"],
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
with oracledb.connect(user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], dsn=os.environ["DB_DSN"]) as connection:
    for message in consumer:
        apply_event(connection, message.value)
        consumer.commit()  # nunca antes do commit no read model
