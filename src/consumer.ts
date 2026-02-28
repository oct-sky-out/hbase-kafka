import { createServer } from "node:http";
import { Kafka, Producer } from "kafkajs";
import { Counter, Histogram, Registry } from "prom-client";
import { parseAuditEvent } from "./types.js";

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
const groupId = process.env.KAFKA_GROUP_ID ?? "audit-log-consumer-ts";
const tableName = process.env.HBASE_TABLE ?? "audit:logs";
const columnFamily = process.env.HBASE_CF ?? "cf";
const hbaseHost = process.env.HBASE_HOST ?? "localhost";
const hbasePort = Number.parseInt(process.env.HBASE_PORT ?? "9090", 10);
const consumerMetricsPort = Number.parseInt(process.env.CONSUMER_METRICS_PORT ?? "9464", 10);
const consumeOnce = process.argv.includes("--once");

const hbaseClient = hbase({
  host: hbaseHost,
  port: Number.isNaN(hbasePort) ? 9090 : hbasePort
});

const metricsRegistry = new Registry();
const consumerProcessedTotal = new Counter({
  name: "audit_consumer_processed_total",
  help: "Total processed messages by consumer",
  labelNames: ["status"] as const,
  registers: [metricsRegistry]
});
const consumerProcessDurationMs = new Histogram({
  name: "audit_consumer_process_duration_ms",
  help: "Consumer message processing duration in milliseconds",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000],
  registers: [metricsRegistry]
});
const consumerDlqSentTotal = new Counter({
  name: "audit_consumer_dlq_sent_total",
  help: "Total messages sent to DLQ by consumer",
  registers: [metricsRegistry]
});

function startMetricsServer(): void {
  if (Number.isNaN(consumerMetricsPort) || consumerMetricsPort <= 0) {
    console.error(`Invalid CONSUMER_METRICS_PORT=${process.env.CONSUMER_METRICS_PORT}`);
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

  server.listen(consumerMetricsPort, () => {
    console.log(`Consumer metrics listening on :${consumerMetricsPort}/metrics`);
  });

  server.on("error", (error) => {
    console.error(`Consumer metrics server error: ${String(error)}`);
  });
}

function parseRetryCount(headerValue: Buffer | string | Array<Buffer | string> | undefined): number {
  if (!headerValue) {
    return 0;
  }

  const normalized = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (!normalized) {
    return 0;
  }

  const parsedValue = Number.parseInt(normalized.toString(), 10);
  return Number.isInteger(parsedValue) && parsedValue >= 0 ? parsedValue : 0;
}

function putRow(targetTable: string, rowKey: string, columns: Record<string, string>): Promise<void> {
  return new Promise((resolve, reject) => {
    const entries = Object.entries(columns);
    const columnNames = entries.map(([column]) => column);
    const columnValues = entries.map(([, value]) => value);

    hbaseClient.table(targetTable).row(rowKey).put(columnNames, columnValues, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function saveEvent(rawValue: string): Promise<string> {
  const event = parseAuditEvent(rawValue);
  const rowKey = `${event.pattern_id}#${event.event_time}#${event.id}`;

  await putRow(tableName, rowKey, {
    [`${columnFamily}:id`]: event.id,
    [`${columnFamily}:pattern_id`]: String(event.pattern_id),
    [`${columnFamily}:action_type`]: event.action_type,
    [`${columnFamily}:event_time`]: String(event.event_time),
    [`${columnFamily}:url`]: event.url
  });

  return rowKey;
}

async function sendToDlq(producer: Producer, rawValue: string, nextRetryCount: number, reason: string): Promise<void> {
  const dlqPayload = JSON.stringify({
    original_message: rawValue,
    retry_count: nextRetryCount,
    reason,
    failed_at: new Date().toISOString()
  });

  await producer.send({
    topic: dlqTopic,
    acks: -1,
    messages: [{ value: dlqPayload }]
  });
}

async function run(): Promise<void> {
  const kafka = new Kafka({
    clientId: "audit-consumer-ts",
    brokers
  });

  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(dlqTopic)) {
    throw new Error(`DLQ topic not found: ${dlqTopic}. Create it first with RF=3 and min.insync.replicas=2.`);
  }
  if (!topics.includes(topic)) {
    throw new Error(`Source topic not found: ${topic}. Create it before running consumer.`);
  }
  await admin.disconnect();
  startMetricsServer();

  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  const producer = kafka.producer({ allowAutoTopicCreation: false });

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let handledCount = 0;
  console.log(`Consumer started for topic=${topic}, groupId=${groupId}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const rawValue = message.value.toString("utf8");
      const retryCount = parseRetryCount(message.headers?.["x-retry-count"]);
      const endTimer = consumerProcessDurationMs.startTimer();

      try {
        const rowKey = await saveEvent(rawValue);
        consumerProcessedTotal.inc({ status: "success" });
        console.log(`Saved to HBase rowKey=${rowKey}`);
      } catch (error) {
        const reason = error instanceof Error ? error.message : "Unknown processing error";
        const nextRetryCount = retryCount + 1;
        await sendToDlq(producer, rawValue, nextRetryCount, reason);
        consumerDlqSentTotal.inc();
        consumerProcessedTotal.inc({ status: "dlq" });
        console.error(`Failed message sent to DLQ topic=${dlqTopic}, retry_count=${nextRetryCount}, reason=${reason}`);
        console.error(`Fail Reason ${error}`)
      } finally {
        endTimer();
        handledCount += 1;
      }

      if (consumeOnce && handledCount >= 1) {
        await consumer.stop();
        await consumer.disconnect();
        await producer.disconnect();
        process.exit(0);
      }
    }
  });
}

run().catch((error) => {
  console.error("Consumer failed:", error);
  process.exit(1);
});
