import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import sitemap from '@astrojs/sitemap';
import preact from '@astrojs/preact';

const SITE = 'https://postcode.page';

export default defineConfig({
  site: SITE,
  output: 'static',

  integrations: [
    sitemap({
      filter(page) {
        // Exclude low-value utility pages from sitemap
        const exclude = ['/privacy/', '/terms/', '/search/'];
        return !exclude.some(path => page.endsWith(path));
      },
      serialize(item) {
        const url = item.url;
        item.lastmod = new Date().toISOString().split('T')[0];

        // Homepage
        if (url === `${SITE}/`) {
          item.priority = 1.0;
          item.changefreq = 'weekly';
          return item;
        }

        // Postcode district pages e.g. /sw1/ /al10/
        if (/\/[a-z]{1,2}\d{1,2}(\/[a-z]{1,2}\d[a-z\d]*)?\/$/i.test(url.replace(SITE, ''))) {
          item.priority = 0.8;
          item.changefreq = 'monthly';
          return item;
        }

        // County / region hub pages
        if (url.includes('/counties/') || url.includes('/regions/')) {
          item.priority = 0.7;
          item.changefreq = 'monthly';
          return item;
        }

        // Blog posts and guides
        if (url.includes('/blog/') || url.includes('/guides/')) {
          item.priority = 0.7;
          item.changefreq = 'monthly';
          return item;
        }

        // Comparison pages
        if (url.includes('/compare/')) {
          item.priority = 0.6;
          item.changefreq = 'monthly';
          return item;
        }

        // Tools, methodology, about
        if (url.includes('/tools/') || url.includes('/methodology/') || url.includes('/about/')) {
          item.priority = 0.5;
          item.changefreq = 'yearly';
          return item;
        }

        // Index pages (blog index, guides index, etc.)
        item.priority = 0.6;
        item.changefreq = 'weekly';
        return item;
      },
    }),
    preact(),
  ],

  vite: {
    plugins: [tailwindcss()],
  },
});
