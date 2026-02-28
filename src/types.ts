export type AuditLogEvent = {
  id: string;
  pattern_id: number;
  action_type: string;
  event_time: number;
  url: string;
};

const patternActionMap: Record<number, string> = {
  0: "move_url",
  2: "create",
  3: "edit",
  4: "delete"
};

export function validatePatternAction(patternId: number, actionType: string): void {
  const expected = patternActionMap[patternId];
  if (!expected) {
    throw new Error(`Unsupported pattern_id: ${patternId}`);
  }
  if (expected !== actionType) {
    throw new Error(`Invalid action_type for pattern_id=${patternId}. expected=${expected}, actual=${actionType}`);
  }
}

export function parseAuditEvent(raw: string): AuditLogEvent {
  const parsed = JSON.parse(raw) as Partial<AuditLogEvent>;

  if (typeof parsed.id !== "string" || parsed.id.length === 0) {
    throw new Error("Invalid id");
  }
  if (typeof parsed.pattern_id !== "number") {
    throw new Error("Invalid pattern_id");
  }
  if (typeof parsed.action_type !== "string" || parsed.action_type.length === 0) {
    throw new Error("Invalid action_type");
  }
  if (typeof parsed.event_time !== "number" || !Number.isInteger(parsed.event_time)) {
    throw new Error("Invalid event_time");
  }
  if (typeof parsed.url !== "string" || parsed.url.length === 0) {
    throw new Error("Invalid url");
  }

  validatePatternAction(parsed.pattern_id, parsed.action_type);

  return {
    id: parsed.id,
    pattern_id: parsed.pattern_id,
    action_type: parsed.action_type,
    event_time: parsed.event_time,
    url: parsed.url
  };
}
