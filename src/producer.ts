import { Kafka } from "kafkajs";
import { randomUUID } from "node:crypto";
import { AuditLogEvent } from "./types.js";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092,localhost:9093,localhost:9094")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const topic = process.env.KAFKA_TOPIC ?? "audit_log";
const eventCountPerRun = 200;

function getRandomIntInclusive(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getEventTypeByRoll(roll: number): Pick<AuditLogEvent, "pattern_id" | "action_type"> {
  if (roll >= 1 && roll <= 400) {
    return { action_type: "move_url", pattern_id: 0 };
  }
  if (roll >= 401 && roll <= 600) {
    return { action_type: "create", pattern_id: 2 };
  }
  if (roll >= 601 && roll <= 800) {
    return { action_type: "edit", pattern_id: 3 };
  }
  return { action_type: "delete", pattern_id: 4 };
}

function buildEvent(): AuditLogEvent {
  const roll = getRandomIntInclusive(1, 1000);
  const eventType = getEventTypeByRoll(roll);
  const eventId = randomUUID();

  return {
    id: eventId,
    pattern_id: eventType.pattern_id,
    action_type: eventType.action_type,
    event_time: Math.floor(Date.now() / 1000),
    url: `https://www.example.com/${eventType.action_type}/${eventId}`
  };
}

async function run(): Promise<void> {
  const kafka = new Kafka({
    clientId: "audit-producer-ts",
    brokers
  });

  const producer = kafka.producer();
  await producer.connect();

  const events = Array.from({ length: eventCountPerRun }, () => buildEvent());
  const sendResults = await Promise.allSettled(
    events.map((event) =>
      producer.send({
        topic,
        acks: -1,
        messages: [
          {
            key: String(event.pattern_id),
            value: JSON.stringify(event)
          }
        ]
      })
    )
  );

  const successCount = sendResults.filter((result) => result.status === "fulfilled").length;
  const failureCount = sendResults.length - successCount;
  console.log(`Produced ${successCount}/${events.length} events asynchronously.`);
  if (failureCount > 0) {
    console.error(`Failed to produce ${failureCount} events.`);
  }

  await producer.disconnect();
}

run().catch((error) => {
  console.error("Producer failed:", error);
  process.exit(1);
});
