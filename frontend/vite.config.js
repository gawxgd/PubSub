import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:5001',
        changeOrigin: true,
        configure: (proxy, _options) => {
          proxy.on('error', (err, _req, _res) => {
            // Silently handle connection errors when backend is not available
            // The UI will show connection status instead
          })
        }
      },
      '/loggerhub': {
        target: 'http://localhost:5001',
        changeOrigin: true,
        ws: true, // Enable WebSocket support for SignalR
        configure: (proxy, _options) => {
          proxy.on('error', (err, _req, _res) => {
            // Silently handle connection errors when backend is not available
            // The UI will show connection status instead
          })
        }
      }
    }
  }
})

