/**
 * config.ts
 * ─────────────────────────────────────────────────────────────────────────────
 * Reads environment variables and provides typed defaults.
 *
 * Mirrors the env-var block in cmd/audit/main.go:
 *   amqpURL := os.Getenv("AMQP_URL")   → AMQP_URL
 *   lokiURL := os.Getenv("LOKI_URL")   → LOKI_URL
 *
 * All values have local-dev defaults so the service runs without a .env file.
 * ─────────────────────────────────────────────────────────────────────────────
 */
export const config = {
    /** RabbitMQ connection string — same default as Go services */
    amqpUrl: process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5672/",
    /** Loki push endpoint — same default as Go audit service */
    lokiUrl: process.env.LOKI_URL ?? "http://localhost:3100",
};
