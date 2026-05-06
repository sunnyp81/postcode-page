import type { APIRoute } from 'astro';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

export const GET: APIRoute = () => {
  const pairs: string[] = JSON.parse(
    readFileSync(join(process.cwd(), 'data', 'comparison-pairs.json'), 'utf8')
  );

  const urls = pairs.map(pair => `  <url>
    <loc>${SITE}/compare/${pair.toLowerCase()}/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.5</priority>
  </url>`);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
