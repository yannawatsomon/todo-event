/**
 * index.ts — Entry point for the Node.js audit service
 * ─────────────────────────────────────────────────────────────────────────────
 * This is the Node.js/TypeScript equivalent of cmd/audit/main.go.
 *
 * Responsibilities (identical to the Go version):
 *   1. Connect to RabbitMQ
 *   2. Subscribe to "task.events"       → queue "audit.task.events"
 *   3. Subscribe to "onboarding.events" → queue "audit.user.events"
 *   4. For every message received: push it to Loki via HTTP POST
 *   5. Gracefully shut down on SIGINT / SIGTERM
 *
 * How the pieces fit together:
 *
 *   cmd/api (Go)          publishes → task.events (RabbitMQ fanout)
 *   cmd/onboarding (Go)   publishes → onboarding.events (RabbitMQ fanout)
 *                                          │
 *                              ┌───────────┴───────────┐
 *                              ▼                       ▼
 *                    audit.task.events       audit.user.events
 *                              │                       │
 *                              └───────────┬───────────┘
 *                                          ▼
 *                                   index.ts (this file)
 *                                          │
 *                                          ▼
 *                                   pushToLoki()
 *                                          │
 *                                          ▼
 *                              POST /loki/api/v1/push
 * ─────────────────────────────────────────────────────────────────────────────
 */
import { config } from "./config.js";
import { connect, subscribe, EXCHANGES, QUEUES, } from "./messaging.js";
import { pushToLoki } from "./loki.js";
// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
    console.log("[audit] starting Node.js audit service");
    console.log("[audit] AMQP_URL:", config.amqpUrl);
    console.log("[audit] LOKI_URL:", config.lokiUrl);
    // ── Step 1: Connect to RabbitMQ ───────────────────────────────────────────
    // Equivalent to: conn, ch, err := messaging.Connect(amqpURL)
    const { conn, ch } = await connect(config.amqpUrl);
    console.log("[audit] connected to RabbitMQ");
    // ── Step 2: Build the shared event handler ────────────────────────────────
    // Both subscriptions do the same thing: log the event type and push to Loki.
    // Extracted into a function to avoid duplication (DRY).
    //
    // Equivalent to the inline handler in Go:
    //   func(msg messaging.Message) {
    //     slog.Info("audit: received event", "type", msg.Type)
    //     pushToLoki(lokiURL, msg.Type, msg.Payload)
    //   }
    async function handleEvent(msg) {
        console.log("[audit] received event:", msg.type);
        await pushToLoki(config.lokiUrl, msg.type, msg.payload);
    }
    // ── Step 3: Subscribe to task events ─────────────────────────────────────
    // Exchange: "task.events"  (published by cmd/api when tasks are created/updated)
    // Queue:    "audit.task.events"
    //
    // Equivalent to:
    //   messaging.Subscribe(ch, messaging.TaskExchange, messaging.QueueAuditTaskEvents, handler)
    await subscribe(ch, EXCHANGES.task, QUEUES.auditTask, handleEvent);
    console.log(`[audit] subscribed to exchange="${EXCHANGES.task}" queue="${QUEUES.auditTask}"`);
    // ── Step 4: Subscribe to onboarding/user events ───────────────────────────
    // Exchange: "onboarding.events"  (published by cmd/onboarding for user.* events)
    // Queue:    "audit.user.events"
    //
    // Equivalent to:
    //   messaging.Subscribe(ch, messaging.OnboardingExchange, messaging.QueueAuditUserEvents, handler)
    await subscribe(ch, EXCHANGES.onboarding, QUEUES.auditUser, handleEvent);
    console.log(`[audit] subscribed to exchange="${EXCHANGES.onboarding}" queue="${QUEUES.auditUser}"`);
    console.log("[audit] service listening — waiting for events...");
    // ── Step 5: Graceful shutdown ─────────────────────────────────────────────
    // Wait for SIGINT (Ctrl+C) or SIGTERM (Docker stop) then close the connection.
    //
    // Equivalent to:
    //   quit := make(chan os.Signal, 1)
    //   signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    //   <-quit
    await new Promise((resolve) => {
        process.once("SIGINT", resolve);
        process.once("SIGTERM", resolve);
    });
    console.log("[audit] shutting down...");
    await conn.close();
    console.log("[audit] stopped");
}
// ── Bootstrap ─────────────────────────────────────────────────────────────────
// Top-level error handler — if startup fails (e.g. RabbitMQ not ready), log and exit.
main().catch((err) => {
    console.error("[audit] fatal error:", err);
    process.exit(1);
});
