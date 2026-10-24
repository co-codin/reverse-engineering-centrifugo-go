import { readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const root = path.dirname(fileURLToPath(import.meta.url))
const channeld = JSON.parse(readFileSync(path.join(root, '../configs/channeld.json'), 'utf8')) as { port: number }

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.join(root, 'src'),
    },
  },
  server: {
    host: '0.0.0.0',
    port: 4327,
    strictPort: true,
    proxy: {
      '/connection': { target: `http://127.0.0.1:${channeld.port}`, ws: true },
      '/api': { target: `http://127.0.0.1:${channeld.port}` },
      '/health': { target: `http://127.0.0.1:${channeld.port}` },
    },
  },
  preview: {
    host: '0.0.0.0',
    port: 4327,
    strictPort: true,
  },
})
