/**
 * loki.ts
 * ─────────────────────────────────────────────────────────────────────────────
 * Pushes a single event log line to Grafana Loki.
 *
 * Mirrors pushToLoki() in cmd/audit/main.go.
 *
 * Loki push API format:
 *   POST /loki/api/v1/push
 *   Content-Type: application/json
 *   Body: {
 *     streams: [{
 *       stream: { label_key: "label_value", ... },   ← used for filtering in Grafana
 *       values: [[ "<unix_nano_timestamp>", "<log_line>" ]]
 *     }]
 *   }
 *
 * Each "stream" is a unique combination of labels.
 * Each "value" is a [timestamp_nanoseconds_string, log_line_string] pair.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ── Types ─────────────────────────────────────────────────────────────────────

/** A single Loki log stream with labels and log lines */
interface LokiStream {
  stream: Record<string, string>; // label key-value pairs for Grafana filtering
  values: [string, string][]; // array of [nanosecond_timestamp, log_line]
}

/** Top-level Loki push request body */
interface LokiPushBody {
  streams: LokiStream[];
}

// ── pushToLoki ────────────────────────────────────────────────────────────────
/**
 * Formats an event as a Loki log entry and POSTs it to the Loki push API.
 *
 * @param lokiUrl   - base URL of Loki, e.g. "http://localhost:3100"
 * @param eventType - event type string, e.g. "task.created"
 * @param payload   - raw event payload (will be JSON-stringified into the log line)
 *
 * @throws if the HTTP request fails or Loki returns a non-2xx status
 */
export async function pushToLoki(
  lokiUrl: string,
  eventType: string,
  payload: unknown,
): Promise<void> {
  // 1. Build the log line — same structure as Go:
  //    json.Marshal(map[string]any{ "event_type": eventType, "payload": payload })
  const logLine = JSON.stringify({ event_type: eventType, payload });

  // 2. Timestamp in nanoseconds as a string (Loki requires nanosecond precision)
  //    Go: fmt.Sprintf("%d", time.Now().UnixNano())
  //    JS: Date.now() gives milliseconds → multiply by 1_000_000 for nanoseconds
  const timestampNano = (BigInt(Date.now()) * 1_000_000n).toString();

  // 3. Assemble the Loki push body
  //    Labels: "service" and "event_type" — same as Go version
  const body: LokiPushBody = {
    streams: [
      {
        stream: {
          service: "audit", // identifies this service in Grafana
          event_type: eventType, // allows filtering by event type in Grafana
        },
        values: [[timestampNano, logLine]],
      },
    ],
  };

  // 4. POST to Loki push endpoint
  const response = await fetch(`${lokiUrl}/loki/api/v1/push`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  console.log("dddd", response);

  // 5. Throw on non-2xx so the caller (subscribe handler) can nack the message
  if (!response.ok) {
    throw new Error(`loki push failed: HTTP ${response.status}`);
  }
}
