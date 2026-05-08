วิธี revert กลับไปใช้ Go version
แค่เปลี่ยนบรรทัดเดียวใน compose.yml:

# Node version (ปัจจุบัน)
dockerfile: cmd/audit-node/Dockerfile

# Go version (เดิม)
dockerfile: cmd/audit/Dockerfile
วิธีรัน
# รัน infrastructure + services ทั้งหมด
docker compose up -d

# หรือรัน Node audit service แบบ local dev
cd cmd/audit-node
bun run dev   # tsx watch — hot reload
bun start     # tsx — run once