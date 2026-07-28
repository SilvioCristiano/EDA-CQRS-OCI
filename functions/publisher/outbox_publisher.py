"""Publica a transactional outbox no tópico Kafka de forma segura para retry.

Execute como um único Deployment OKE (ou múltiplas réplicas com SKIP LOCKED).
Credenciais Kafka e ADB devem vir do Vault, via Workload Identity/CSI.
"""
import json
import os
import time

import oracledb
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
    security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_AUTH_TOKEN"],
    acks="all", retries=8, retry_backoff_ms=250,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda event: json.dumps(event, separators=(",", ":")).encode("utf-8"),
)


def publish_batch(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT event_id, topic, event_key, payload FROM outbox
                       WHERE status = 'PENDING' ORDER BY created_at
                       FETCH FIRST 100 ROWS ONLY FOR UPDATE SKIP LOCKED""")
        rows = cur.fetchall()
        for event_id, topic, key, payload in rows:
            # Espera ACK antes de tornar o evento elegível como publicado.
            producer.send(topic, key=key, value=json.loads(payload.read() if hasattr(payload, "read") else payload)).get(timeout=20)
            cur.execute("UPDATE outbox SET status='PUBLISHED', published_at=SYSTIMESTAMP WHERE event_id=:1", [event_id])
    conn.commit()
    return len(rows)


with oracledb.connect(user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], dsn=os.environ["DB_DSN"]) as connection:
    while True:
        count = publish_batch(connection)
        if count == 0:
            time.sleep(0.5)
