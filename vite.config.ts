import * as path from 'node:path';
import { defineConfig } from 'vite';

export default defineConfig({
  root: path.join(__dirname, 'demo'),
  base: './',
  build: {
    outDir: path.resolve(__dirname, 'dist-demo/'),
    rollupOptions: {
      input: {
        demo: path.resolve(__dirname, 'demo/index.html'),
        worker: path.resolve(__dirname, 'demo/worker.ts'),
      },
      output: {
        format: 'esm',
      },
      external: ['zstddec'],
    },
  },
  // publicDir: 'demo-static',
  worker: {
    format: 'es',
  },
})