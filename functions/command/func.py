"""OCI Function: recebe um comando e o persiste com transactional outbox.

Variáveis: DB_USER, DB_PASSWORD, DB_DSN. O segredo deve ser injetado pelo
Vault/OCI Functions; nunca registre payload, senha ou token em log.
"""
import io
import json
import os
import uuid
import base64
from datetime import datetime, timezone

import oracledb
import oci
from fdk import response


def _reply(ctx, status, body):
    return response.Response(
        ctx,
        response_data=json.dumps(body),
        headers={"Content-Type": "application/json"},
        status_code=status,
    )


def _db_password():
    secret_ocid = os.environ.get("DB_PASSWORD_SECRET_OCID")
    if not secret_ocid:
        raise RuntimeError("DB_PASSWORD_SECRET_OCID é obrigatório; senha em variável de ambiente é proibida")
    signer = oci.auth.signers.get_resource_principals_signer()
    content = oci.secrets.SecretsClient({}, signer=signer).get_secret_bundle(secret_ocid).data.secret_bundle_content.content
    return base64.b64decode(content).decode("utf-8")


def handler(ctx, data: io.BytesIO = None):
    try:
        command = json.loads(data.getvalue())
        headers = dict(ctx.Headers())
        idempotency_key = headers.get("idempotency-key")
        correlation_id = headers.get("x-correlation-id", str(uuid.uuid4()))
        if not idempotency_key or not command.get("orderId") or not command.get("customerId"):
            return _reply(ctx, 400, {"code": "INVALID_COMMAND", "message": "orderId, customerId e Idempotency-Key são obrigatórios"})

        event_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        with oracledb.connect(
            user=os.environ["DB_USER"], password=_db_password(), dsn=os.environ["DB_DSN"]
        ) as conn:
            with conn.cursor() as cur:
                # A constraint UNIQUE em commands.idempotency_key torna a repetição segura.
                cur.execute("SELECT event_id FROM commands WHERE idempotency_key = :1", [idempotency_key])
                existing = cur.fetchone()
                if existing:
                    return _reply(ctx, 202, {"status": "accepted", "eventId": existing[0], "duplicate": True})

                cur.execute(
                    "INSERT INTO orders (order_id, customer_id, status, version, created_at) VALUES (:1,:2,'CREATED',1,SYSTIMESTAMP)",
                    [command["orderId"], command["customerId"]],
                )
                event = {
                    "eventId": event_id, "eventType": "com.acme.orders.order-created", "eventVersion": 1,
                    "occurredAt": now, "aggregateId": command["orderId"], "aggregateVersion": 1,
                    "correlationId": correlation_id, "payload": command,
                }
                cur.execute("INSERT INTO commands (idempotency_key, event_id) VALUES (:1,:2)", [idempotency_key, event_id])
                cur.execute(
                    "INSERT INTO outbox (event_id, topic, event_key, payload, status, created_at) VALUES (:1,:2,:3,:4,'PENDING',SYSTIMESTAMP)",
                    [event_id, os.environ["KAFKA_TOPIC"], command["orderId"], json.dumps(event)],
                )
            conn.commit()
        return _reply(ctx, 202, {"status": "accepted", "eventId": event_id, "correlationId": correlation_id})
    except json.JSONDecodeError:
        return _reply(ctx, 400, {"code": "INVALID_JSON"})
    except oracledb.Error:
        # Logue apenas correlationId e o tipo do erro em Logging.
        return _reply(ctx, 503, {"code": "WRITE_MODEL_UNAVAILABLE"})
