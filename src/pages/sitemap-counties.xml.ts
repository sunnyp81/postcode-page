import type { APIRoute } from 'astro';
import { getAllCountySlugs, getAllRegions } from '../utils/data';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

export const GET: APIRoute = () => {
  const countySlugs = getAllCountySlugs();
  const regions = getAllRegions();

  const countyUrls = countySlugs.map(slug => `  <url>
    <loc>${SITE}/counties/${slug}/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>`);

  const regionUrls = regions.map(r => `  <url>
    <loc>${SITE}/regions/${r.slug}/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>`);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${countyUrls.join('\n')}
${regionUrls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
