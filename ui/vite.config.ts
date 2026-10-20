import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The same /api routes nginx serves in the cluster (nginx.conf.template)
const query = process.env.QUERY_UPSTREAM ?? "http://localhost:8090";
const ingest = process.env.INGEST_UPSTREAM ?? "http://localhost:4318";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api/query": { target: query, changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/query/, "") },
      "/api/ingest": { target: ingest, changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/ingest/, "") },
    },
  },
  test: { environment: "node" },
});
