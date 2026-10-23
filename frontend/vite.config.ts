import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        ws: true,
      },
      "/health": "http://127.0.0.1:8080",
    },
  },
  build: {
    outDir: "dist",
  },
});
