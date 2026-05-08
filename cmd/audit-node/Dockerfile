# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile for cmd/audit-node
#
# Uses the official Bun image — Bun can run TypeScript directly via `bun run`,
# so no separate tsx or compile step is needed inside the container.
#
# Build context: repo root (same as other services in compose.yml)
# ─────────────────────────────────────────────────────────────────────────────

FROM oven/bun:1 AS base

WORKDIR /app

# ── Install dependencies ──────────────────────────────────────────────────────
# Copy only package files first so Docker can cache this layer.
# If source files change but dependencies don't, this layer is reused.
COPY cmd/audit-node/package.json cmd/audit-node/bun.lock* ./

RUN bun install --frozen-lockfile

# ── Copy source ───────────────────────────────────────────────────────────────
COPY cmd/audit-node/src ./src
COPY cmd/audit-node/tsconfig.json ./

# ── Run ───────────────────────────────────────────────────────────────────────
# Bun runs TypeScript natively — no build step required.
# AMQP_URL and LOKI_URL are injected by compose.yml at runtime.
CMD ["bun", "run", "src/index.ts"]
