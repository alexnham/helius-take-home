// One JSON object per line on stdout: ts, level, event, then the event's fields.
export function log(level: "info" | "warn" | "error", event: string, fields: Record<string, unknown> = {}) {
  // bigint isn't JSON; slots are well under 2^53.
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }, (_, v) => (typeof v === "bigint" ? Number(v) : v)));
}
