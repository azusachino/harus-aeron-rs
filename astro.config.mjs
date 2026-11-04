import { defineConfig } from 'astro/config';
export default defineConfig({ site: 'https://harus-aeron-rs.local', trailingSlash: 'never', redirects: { '/tutorial/': '/tutorial/README' }, markdown: { shikiConfig: { theme: 'github-light' } } });
