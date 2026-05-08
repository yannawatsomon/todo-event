import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      "/api/users": {
        // cmd/onboarding runs on port 3003 (local go run)
        target: "http://localhost:3003",
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      "/api": {
        // cmd/api runs on port 3000
        target: "http://localhost:3000",
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      "/captcha": {
        // captcha is served by cmd/onboarding on port 3003
        target: "http://localhost:3003",
      },
    },
  },
});
