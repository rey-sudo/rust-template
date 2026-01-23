
CREATE DATABASE service_ingest WITH ENCODING = 'UTF8';

CREATE USER service_ingest_user WITH PASSWORD 'password';

GRANT CONNECT ON DATABASE service_ingest TO service_ingest_user;

\c service_ingest

GRANT USAGE ON SCHEMA public TO service_ingest_user;
GRANT CREATE ON SCHEMA public TO service_ingest_user;

GRANT SELECT, INSERT, UPDATE ON TABLE ohlcv_1m TO service_ingest_user;
