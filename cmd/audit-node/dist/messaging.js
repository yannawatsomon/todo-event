/**
 * messaging.ts
 * ─────────────────────────────────────────────────────────────────────────────
 * RabbitMQ helpers — mirrors internal/messaging/rabbit.go
 *
 * Responsibilities:
 *   1. connect()        — open AMQP connection + channel
 *   2. subscribe()      — declare exchange + queue + binding, then consume
 *
 * Key design decisions (same as Go side):
 *   - Fanout exchange: every message published to an exchange is broadcast
 *     to ALL queues bound to it — audit gets every event automatically.
 *   - Durable queues: messages survive RabbitMQ restarts.
 *   - Manual ack: we only ack after the handler succeeds (pushToLoki).
 *     If the handler throws, the message is nack'd and stays in the queue.
 * ─────────────────────────────────────────────────────────────────────────────
 */
import amqp from "amqplib";
// ── Exchange & Queue name constants ──────────────────────────────────────────
// Must match the constants in internal/messaging/rabbit.go and message.go
export const EXCHANGES = {
    task: "task.events", // published by cmd/api  (task.created, task.status_changed)
    onboarding: "onboarding.events", // published by cmd/onboarding (user.* events)
};
export const QUEUES = {
    auditTask: "audit.task.events", // this service consumes task events
    auditUser: "audit.user.events", // this service consumes onboarding/user events
};
// ── connect ───────────────────────────────────────────────────────────────────
/**
 * Opens an AMQP connection and a single channel.
 * Equivalent to messaging.Connect() in rabbit.go.
 */
export async function connect(url) {
    const conn = await amqp.connect(url);
    const ch = await conn.createChannel();
    return { conn, ch };
}
// ── subscribe ─────────────────────────────────────────────────────────────────
/**
 * Declares a fanout exchange, a durable queue, binds them together,
 * then starts consuming messages in the background.
 *
 * Equivalent to messaging.Subscribe() in rabbit.go.
 *
 * @param ch        - AMQP channel
 * @param exchange  - fanout exchange name (e.g. "task.events")
 * @param queue     - durable queue name   (e.g. "audit.task.events")
 * @param handler   - called for each message; throw to nack, return to ack
 */
export async function subscribe(ch, exchange, queue, handler) {
    // 1. Declare the fanout exchange (idempotent — safe to call multiple times)
    await ch.assertExchange(exchange, "fanout", { durable: true });
    // 2. Declare a durable queue so messages survive broker restarts
    await ch.assertQueue(queue, { durable: true });
    // 3. Bind the queue to the exchange (no routing key needed for fanout)
    await ch.bindQueue(queue, exchange, "");
    // 4. Start consuming — prefetch 1 so we process one message at a time
    await ch.prefetch(1);
    await ch.consume(queue, async (delivery) => {
        if (!delivery)
            return; // consumer cancelled
        try {
            // Parse the JSON envelope: { type: string, payload: unknown }
            const msg = JSON.parse(delivery.content.toString());
            await handler(msg);
            // Ack only after handler succeeds (mirrors d.Ack(false) in Go)
            ch.ack(delivery);
        }
        catch (err) {
            console.error("[rabbit] handler error — nacking message", err);
            // Nack without requeue to avoid infinite retry loops
            ch.nack(delivery, false, false);
        }
    });
}
