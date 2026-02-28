# hbase-kafka ローカル実習

- 日本語版
- English (default): [`README.md`](README.md)
- 한국어: [`README.ko.md`](README.ko.md)

Kafka（3ブローカー、KRaft）+ HBase（1ノード）+ TypeScript Producer/Consumer + 監視（Prometheus/Grafana/Loki）をローカルで動かす最小構成です。

## アーキテクチャ

`actions -> Kafka producer -> Kafka brokers -> consumer -> HBase(audit:logs) -> DLQ retry -> HBase(audit:fail_logs)`

## 前提条件

- Docker + Docker Compose
- Node.js 20+
- `pnpm`

## 1) イメージを pull

```bash
docker compose pull
```

## 2) コンテナ起動

```bash
docker compose up -d
```

## 3) 状態確認

```bash
docker compose ps
docker compose logs -f kafka-1
docker compose logs -f kafka-2
docker compose logs -f kafka-3
docker compose logs -f hbase
docker compose logs -f kafka-ui
```

## 4) 停止

```bash
docker compose down
```

## ポート

- Kafka broker-1 external: `localhost:9092`
- Kafka broker-2 external: `localhost:9093`
- Kafka broker-3 external: `localhost:9094`
- Kafka internal bootstrap: `kafka-1:29092,kafka-2:29092,kafka-3:29092`
- HBase Master UI: `http://localhost:16010`
- HBase REST（node-hbase 接続先）: `http://localhost:9090`（container `8080`）
- Kafka UI: `http://localhost:8080`
- Kafka Exporter: `http://localhost:9308/metrics`
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000`
- Loki: `http://localhost:3100`
- Promtail metrics: `http://localhost:9080/metrics`

## Kafka トピック設定（推奨）

この 3 ブローカー環境では、3 レプリカ + `min.insync.replicas=2` を推奨します。

```bash
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --topic audit_log --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --topic audit_log_dlq --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic audit_log
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic audit_log_dlq
```

## HBase テーブル作成

shell に接続:

```bash
docker exec -it hbase hbase shell
```

テーブル作成:

```ruby
create_namespace 'audit'
create 'audit:logs', 'cf'
create 'audit:fail_logs', 'cf'
```

## TypeScript アプリ

依存関係インストール:

```bash
pnpm install
```

Producer 実行（1回で 200 件を非同期送信）:

```bash
pnpm producer
```

Consumer 実行（継続購読）:

```bash
pnpm consumer
```

Consumer を 1 件だけ処理して終了:

```bash
pnpm consumer:once
```

DLQ 再試行ワーカー実行（`audit_log_dlq -> audit_log`、最大回数は環境変数で制御）:

```bash
pnpm dlq:retry
```

HBase JMX メトリクス exporter 実行:

```bash
pnpm hbase:metrics
```

`audit:logs` をスキャン（最大 50 行）:

```bash
pnpm hbase:scan
```

次ページをスキャン:

```bash
pnpm hbase:scan -- --start-row "<row_key>"
```

fail テーブルをスキャン:

```bash
HBASE_TABLE=audit:fail_logs pnpm hbase:scan
```

## 監視と内部ログ

監視スタック起動:

```bash
docker compose up -d kafka-exporter prometheus grafana loki promtail
```

アプリのメトリクス:

- `consumer`: `http://localhost:9464/metrics`
- `dlq-retry`: `http://localhost:9465/metrics`
- `hbase-metrics`: `http://localhost:9466/metrics`

Grafana ログイン:

- id: `admin`
- pw: `admin`

Grafana で内部ログを見る:

- `Explore` でデータソース `Loki` を選択
- 全サービス例: `{compose_project="hbase-kafka"}`
- サービス別例: `{service="kafka-1"}`, `{service="hbase"}`, `{service="kafka-ui"}`

## デフォルト環境変数

- `KAFKA_BROKERS`（default: `localhost:9092,localhost:9093,localhost:9094`）
- `KAFKA_TOPIC`（default: `audit_log`）
- `KAFKA_DLQ_TOPIC`（default: `audit_log_dlq`）
- `KAFKA_GROUP_ID`（default: `audit-log-consumer-ts`）
- `KAFKA_DLQ_GROUP_ID`（default: `audit-log-dlq-retry-ts`）
- `HBASE_TABLE`（default: `audit:logs`）
- `HBASE_FAIL_TABLE`（default: `audit:fail_logs`）
- `HBASE_CF`（default: `cf`）
- `HBASE_HOST`（default: `localhost`）
- `HBASE_PORT`（default: `9090`）
- `CONSUMER_METRICS_PORT`（default: `9464`）
- `DLQ_RETRY_METRICS_PORT`（default: `9465`）
- `HBASE_METRICS_PORT`（default: `9466`）
- `HBASE_JMX_URL`（default: `http://localhost:16010/jmx`）
- `CONSUMER_MAX_RETRIES`（default: `3`）

## メッセージ検証と再試行ルール

- `id` は必須（重複防止用ユニーク ID）
- `pattern_id` ↔ `action_type` のマッピング検証:
  - `0 -> move_url`
  - `2 -> create`
  - `3 -> edit`
  - `4 -> delete`
- HBase rowkey 形式: `pattern_id#event_time#id`
- Consumer で失敗した場合:
  - HBase には書き込まない
  - `audit_log_dlq` へ送信
- `pnpm dlq:retry` が DLQ から `audit_log` へ再投入
- 最大回数超過後は HBase `audit:fail_logs` へ保存

## よくあるエラー

次のエラーが出る場合:

`Messages are rejected since there are fewer in-sync replicas than required`

トピック設定を確認してください:

- `replication-factor >= min.insync.replicas` が必要
- このリポジトリの推奨値は `RF=3`, `min.insync.replicas=2`
