import type { APIRoute } from 'astro';
import { getAllPostcodeSlugs } from '../utils/data';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

export const GET: APIRoute = () => {
  const slugs = getAllPostcodeSlugs();
  const urls = slugs.map(slug => `  <url>
    <loc>${SITE}/${slug}/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>`);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
