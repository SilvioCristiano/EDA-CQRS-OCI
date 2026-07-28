"""Query API temporária sobre a projeção. Em produção substitua a consulta
por Oracle NoSQL SDK quando o read model estiver em NoSQL."""
import io
import json
import os
import base64

import oracledb
import oci
from fdk import response


def handler(ctx, data: io.BytesIO = None):
    order_id = ctx.RequestURL().rsplit("/", 1)[-1]
    signer = oci.auth.signers.get_resource_principals_signer()
    secret = oci.secrets.SecretsClient({}, signer=signer).get_secret_bundle(os.environ["DB_PASSWORD_SECRET_OCID"]).data.secret_bundle_content.content
    with oracledb.connect(user=os.environ["DB_USER"], password=base64.b64decode(secret).decode("utf-8"), dsn=os.environ["DB_DSN"]) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload, version, updated_at FROM order_read_model WHERE order_id=:1", [order_id])
            row = cur.fetchone()
    if not row:
        return response.Response(ctx, response_data=json.dumps({"code": "NOT_FOUND"}), status_code=404)
    payload = row[0].read() if hasattr(row[0], "read") else row[0]
    return response.Response(ctx, response_data=json.dumps({"data": json.loads(payload), "version": row[1], "updatedAt": row[2].isoformat()}), headers={"Content-Type": "application/json"})
