import { defineConfig } from 'vite';

// GitHub Pages serves the demo under /workflow-world-browser/; locally it is /.
export default defineConfig({
  base: process.env.DEMO_BASE ?? '/',
  build: { target: 'es2023' },
});
