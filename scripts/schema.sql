-- Reexecutável: ignora somente ORA-00955 (objeto já existe).
CREATE OR REPLACE PROCEDURE ddl_if_absent(sql_text IN VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE sql_text;
EXCEPTION WHEN OTHERS THEN
  IF SQLCODE != -955 THEN RAISE; END IF;
END;
/
BEGIN ddl_if_absent('CREATE TABLE orders (order_id VARCHAR2(64) PRIMARY KEY, customer_id VARCHAR2(64) NOT NULL, status VARCHAR2(30) NOT NULL, version NUMBER NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL)'); END;
/
BEGIN ddl_if_absent('CREATE TABLE commands (idempotency_key VARCHAR2(128) PRIMARY KEY, event_id VARCHAR2(36) NOT NULL)'); END;
/
BEGIN ddl_if_absent('CREATE TABLE outbox (event_id VARCHAR2(36) PRIMARY KEY, topic VARCHAR2(200) NOT NULL, event_key VARCHAR2(200) NOT NULL, payload CLOB NOT NULL, status VARCHAR2(20) NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL, published_at TIMESTAMP WITH TIME ZONE)'); END;
/
BEGIN ddl_if_absent('CREATE INDEX outbox_pending_ix ON outbox(status, created_at)'); END;
/
BEGIN ddl_if_absent('CREATE TABLE processed_events (event_id VARCHAR2(36) PRIMARY KEY, processed_at TIMESTAMP WITH TIME ZONE NOT NULL)'); END;
/
BEGIN ddl_if_absent('CREATE TABLE order_read_model (order_id VARCHAR2(64) PRIMARY KEY, customer_id VARCHAR2(64), version NUMBER NOT NULL, payload CLOB, updated_at TIMESTAMP WITH TIME ZONE NOT NULL)'); END;
/
DROP PROCEDURE ddl_if_absent;
