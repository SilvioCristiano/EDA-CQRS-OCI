# Contratos de integração

## Convenção

- Recursos: `edacqrs-<ambiente>-<serviço>`; streams: `<domínio>.<classe>.v<major>`; consumer groups: `<projeção>-v<major>`.
- Exemplo: `orders.domain.v1`, `orders-read-model-v1`, `edacqrs-prd-command`.
- Use chave Kafka igual ao `aggregateId`. Assim todos os eventos de um agregado vão para a mesma partição e preservam ordem **por agregado**, não entre agregados.

## Comando HTTP

```http
POST /v1/orders
Authorization: Bearer <JWT>
Idempotency-Key: 3c45f5c5-f3fc-4e55-8c84-7e64a6d7c727
X-Correlation-Id: 91e2d7f0-4f0c-4d9a-b1bc-d36d14a36be9
traceparent: 00-<trace-id>-<span-id>-01
Content-Type: application/json

{"orderId":"ord_123","customerId":"cus_345","items":[{"sku":"SKU-1","quantity":2}]}
```

Retorne `202 Accepted` com `eventId` e `correlationId`. Validação, autorização por escopo e limite de tamanho ocorrem no gateway e são repetidos no command service.

## Envelope de evento

```json
{
  "eventId": "c50af050-4d01-4a55-91a0-bb9d77f841a4",
  "eventType": "com.acme.orders.order-created",
  "eventVersion": 1,
  "occurredAt": "2026-07-27T14:00:00Z",
  "aggregateType": "Order",
  "aggregateId": "ord_123",
  "aggregateVersion": 1,
  "correlationId": "91e2d7f0-4f0c-4d9a-b1bc-d36d14a36be9",
  "traceparent": "00-...",
  "producer": "orders-command",
  "payload": {"orderId":"ord_123","customerId":"cus_345"}
}
```

Inclua somente dados necessários; PII deve ser minimizada/tokenizada. Registre o schema no Schema Registry (Avro/Protobuf preferível a JSON em produção) e permita apenas mudanças compatíveis para trás dentro da mesma major. Evento incompatível exige novo tópico `v2` e dupla publicação/migração planejada.

## Retry, deduplicação e falhas

1. Produtor usa `acks=all`, idempotência, timeout e backoff exponencial com jitter.
2. O write side grava o agregado, a chave de idempotência e a outbox na **mesma transação**. Um worker publica a outbox e marca `PUBLISHED` apenas após ACK.
3. Consumidor só confirma offset após gravar a projeção. `processed_events(event_id)` com chave única elimina duplicatas; a versão do agregado impede regressão por replay.
4. Após N tentativas, publique em `<domínio>.dlq.v1`, com erro e tópico/partição/offset originais. Alarme sobre DLQ; reprocessamento é uma ação controlada.
