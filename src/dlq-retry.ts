import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { Kafka } from "kafkajs";
import { Counter, Histogram, Registry } from "prom-client";

const hbase = require("hbase") as (config: { host: string; port: number }) => {
  table: (name: string) => {
    row: (rowKey: string) => {
      put: (
        columns: string | string[],
        values: string | string[],
        callback: (error: unknown) => void
      ) => void;
    };
  };
};

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092,localhost:9093,localhost:9094")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const topic = process.env.KAFKA_TOPIC ?? "audit_log";
const dlqTopic = process.env.KAFKA_DLQ_TOPIC ?? "audit_log_dlq";
const groupId = process.env.KAFKA_DLQ_GROUP_ID ?? "audit-log-dlq-retry-ts";
const failTableName = process.env.HBASE_FAIL_TABLE ?? "audit:fail_logs";
const columnFamily = process.env.HBASE_CF ?? "cf";
const hbaseHost = process.env.HBASE_HOST ?? "localhost";
const hbasePort = Number.parseInt(process.env.HBASE_PORT ?? "9090", 10);
const configuredRetries = Number(process.env.CONSUMER_MAX_RETRIES ?? 3);
const maxRetries = Number.isInteger(configuredRetries) && configuredRetries > 0 ? configuredRetries : 3;
const dlqRetryMetricsPort = Number.parseInt(process.env.DLQ_RETRY_METRICS_PORT ?? "9465", 10);

const hbaseClient = hbase({
  host: hbaseHost,
  port: Number.isNaN(hbasePort) ? 9090 : hbasePort
});

const metricsRegistry = new Registry();
const dlqRetryProcessedTotal = new Counter({
  name: "audit_dlq_retry_processed_total",
  help: "Total processed messages by DLQ retry worker",
  labelNames: ["status"] as const,
  registers: [metricsRegistry]
});
const dlqRetryProcessDurationMs = new Histogram({
  name: "audit_dlq_retry_process_duration_ms",
  help: "DLQ retry worker message processing duration in milliseconds",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000],
  registers: [metricsRegistry]
});

function startMetricsServer(): void {
  if (Number.isNaN(dlqRetryMetricsPort) || dlqRetryMetricsPort <= 0) {
    console.error(`Invalid DLQ_RETRY_METRICS_PORT=${process.env.DLQ_RETRY_METRICS_PORT}`);
    return;
  }

  const server = createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("Not Found");
      return;
    }

    response.statusCode = 200;
    response.setHeader("Content-Type", metricsRegistry.contentType);
    response.end(await metricsRegistry.metrics());
  });

  server.listen(dlqRetryMetricsPort, () => {
    console.log(`DLQ retry metrics listening on :${dlqRetryMetricsPort}/metrics`);
  });

  server.on("error", (error) => {
    console.error(`DLQ retry metrics server error: ${String(error)}`);
  });
}

type DlqPayload = {
  original_message: string;
  retry_count: number;
  reason?: string;
  failed_at?: string;
};

function putFailRow(rowKey: string, columns: Record<string, string>): Promise<void> {
  return new Promise((resolve, reject) => {
    const entries = Object.entries(columns);
    const columnNames = entries.map(([column]) => column);
    const columnValues = entries.map(([, value]) => value);

    hbaseClient.table(failTableName).row(rowKey).put(columnNames, columnValues, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function saveFailedEvent(rawValue: string, reason: string, retries: number): Promise<string> {
  const failId = randomUUID();
  const failedAtEpochSeconds = Math.floor(Date.now() / 1000);
  const rowKey = `fail#${failedAtEpochSeconds}#${failId}`;

  await putFailRow(rowKey, {
    [`${columnFamily}:failed_at`]: String(failedAtEpochSeconds),
    [`${columnFamily}:reason`]: reason,
    [`${columnFamily}:retry_count`]: String(retries),
    [`${columnFamily}:original_message`]: rawValue
  });

  return rowKey;
}

function parseDlqPayload(rawValue: string): DlqPayload {
  const parsed = JSON.parse(rawValue) as Partial<DlqPayload>;
  if (typeof parsed.original_message !== "string" || parsed.original_message.length === 0) {
    throw new Error("DLQ payload missing original_message");
  }
  if (typeof parsed.retry_count !== "number" || !Number.isInteger(parsed.retry_count) || parsed.retry_count < 1) {
    throw new Error("DLQ payload missing retry_count");
  }

  return {
    original_message: parsed.original_message,
    retry_count: parsed.retry_count,
    reason: parsed.reason,
    failed_at: parsed.failed_at
  };
}

async function run(): Promise<void> {
  const kafka = new Kafka({
    clientId: "audit-dlq-retry-ts",
    brokers
  });

  const consumer = kafka.consumer({ groupId });
  const producer = kafka.producer();

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: dlqTopic, fromBeginning: true });
  startMetricsServer();

  console.log(`DLQ retry worker started for topic=${dlqTopic}, target=${topic}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const rawValue = message.value.toString("utf8");
      const endTimer = dlqRetryProcessDurationMs.startTimer();
      try {
        const payload = parseDlqPayload(rawValue);

        if (payload.retry_count > maxRetries) {
          const failRowKey = await saveFailedEvent(payload.original_message, "Retry count exceeded max retries", payload.retry_count);
          dlqRetryProcessedTotal.inc({ status: "failed_to_hbase" });
          console.error(`DLQ message exceeded retries; saved to fail table rowKey=${failRowKey}`);
          return;
        }

        await producer.send({
          topic,
          acks: -1,
          messages: [
            {
              value: payload.original_message,
              headers: {
                "x-retry-count": Buffer.from(String(payload.retry_count))
              }
            }
          ]
        });

        dlqRetryProcessedTotal.inc({ status: "requeued" });
        console.log(`Re-published DLQ message to ${topic}, retry=${payload.retry_count}/${maxRetries}`);
      } catch (error) {
        const reason = error instanceof Error ? error.message : "Unknown DLQ processing error";
        const failRowKey = await saveFailedEvent(rawValue, reason, maxRetries);
        dlqRetryProcessedTotal.inc({ status: "invalid_to_hbase" });
        console.error(`Invalid DLQ message saved to fail table rowKey=${failRowKey}, reason=${reason}`);
      } finally {
        endTimer();
      }
    }
  });
}

run().catch((error) => {
  console.error("DLQ retry worker failed:", error);
  process.exit(1);
});
