# hbase-kafka local practice

- English (default)
- Japanese: see [`README.ja.md`](README.ja.md)
- Korean: see [`README.ko.md`](README.ko.md)

Minimal local stack for Kafka (3 brokers, KRaft) + HBase (1 node) + TypeScript producer/consumer + monitoring (Prometheus/Grafana/Loki).

## Architecture

`actions -> Kafka producer -> Kafka brokers -> consumer -> HBase(audit:logs) -> DLQ retry -> HBase(audit:fail_logs)`

## Prerequisites

- Docker + Docker Compose
- Node.js 20+
- `pnpm`

## 1) Pull images

```bash
docker compose pull
```

## 2) Start containers

```bash
docker compose up -d
```

## 3) Check status

```bash
docker compose ps
docker compose logs -f kafka-1
docker compose logs -f kafka-2
docker compose logs -f kafka-3
docker compose logs -f hbase
docker compose logs -f kafka-ui
```

## 4) Stop containers

```bash
docker compose down
```

## Ports

- Kafka broker-1 external: `localhost:9092`
- Kafka broker-2 external: `localhost:9093`
- Kafka broker-3 external: `localhost:9094`
- Kafka internal bootstrap: `kafka-1:29092,kafka-2:29092,kafka-3:29092`
- HBase Master UI: `http://localhost:16010`
- HBase REST (node-hbase endpoint): `http://localhost:9090` (container `8080`)
- Kafka UI: `http://localhost:8080`
- Kafka Exporter: `http://localhost:9308/metrics`
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000`
- Loki: `http://localhost:3100`
- Promtail metrics: `http://localhost:9080/metrics`

## Kafka topic setup (recommended)

Use 3 replicas and `min.insync.replicas=2` for this 3-broker local cluster:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --topic audit_log --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --topic audit_log_dlq --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic audit_log
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic audit_log_dlq
```

## HBase table setup

Connect to shell:

```bash
docker exec -it hbase hbase shell
```

Create tables:

```ruby
create_namespace 'audit'
create 'audit:logs', 'cf'
create 'audit:fail_logs', 'cf'
```

## TypeScript apps

Install dependencies:

```bash
pnpm install
```

Run producer (sends 200 async events per run):

```bash
pnpm producer
```

Run consumer (continuous subscribe):

```bash
pnpm consumer
```

Run consumer once (process 1 message and exit):

```bash
pnpm consumer:once
```

Run DLQ retry worker (`audit_log_dlq -> audit_log`, max retry controlled by env):

```bash
pnpm dlq:retry
```

Run HBase JMX metrics exporter:

```bash
pnpm hbase:metrics
```

Scan `audit:logs` (max 50 rows):

```bash
pnpm hbase:scan
```

Scan next page:

```bash
pnpm hbase:scan -- --start-row "<row_key>"
```

Scan fail table:

```bash
HBASE_TABLE=audit:fail_logs pnpm hbase:scan
```

## Monitoring and internal logs

Start monitoring stack:

```bash
docker compose up -d kafka-exporter prometheus grafana loki promtail
```

App metrics endpoints:

- `consumer`: `http://localhost:9464/metrics`
- `dlq-retry`: `http://localhost:9465/metrics`
- `hbase-metrics`: `http://localhost:9466/metrics`

Grafana login:

- id: `admin`
- pw: `admin`

View internal logs in Grafana:

- Go to `Explore` and choose data source `Loki`
- All services example query: `{compose_project="hbase-kafka"}`
- Single service examples: `{service="kafka-1"}`, `{service="hbase"}`, `{service="kafka-ui"}`

## Default environment variables

- `KAFKA_BROKERS` (default: `localhost:9092,localhost:9093,localhost:9094`)
- `KAFKA_TOPIC` (default: `audit_log`)
- `KAFKA_DLQ_TOPIC` (default: `audit_log_dlq`)
- `KAFKA_GROUP_ID` (default: `audit-log-consumer-ts`)
- `KAFKA_DLQ_GROUP_ID` (default: `audit-log-dlq-retry-ts`)
- `HBASE_TABLE` (default: `audit:logs`)
- `HBASE_FAIL_TABLE` (default: `audit:fail_logs`)
- `HBASE_CF` (default: `cf`)
- `HBASE_HOST` (default: `localhost`)
- `HBASE_PORT` (default: `9090`)
- `CONSUMER_METRICS_PORT` (default: `9464`)
- `DLQ_RETRY_METRICS_PORT` (default: `9465`)
- `HBASE_METRICS_PORT` (default: `9466`)
- `HBASE_JMX_URL` (default: `http://localhost:16010/jmx`)
- `CONSUMER_MAX_RETRIES` (default: `3`)

## Message validation and retry rules

- `id` is required (deduplication-safe unique identifier)
- `pattern_id` ↔ `action_type` mapping validation:
  - `0 -> move_url`
  - `2 -> create`
  - `3 -> edit`
  - `4 -> delete`
- HBase rowkey format: `pattern_id#event_time#id`
- Consumer failure flow:
  - do not write to HBase
  - send to `audit_log_dlq`
- `pnpm dlq:retry` republishes from DLQ to `audit_log`
- After max retries, message is written to HBase `audit:fail_logs`

## Common error

If you see:

`Messages are rejected since there are fewer in-sync replicas than required`

check topic consistency:

- `replication-factor` must be `>= min.insync.replicas`
- for this repo: use `RF=3`, `min.insync.replicas=2`
