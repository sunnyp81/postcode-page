import type { APIRoute } from 'astro';
import { getAllCountySlugs } from '../utils/data';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

// Static guide pages
const staticGuides = [
  '/guides/',
  '/guides/average-house-price-uk/',
  '/guides/best-areas-for-families/',
  '/guides/cheapest-places-to-live-england/',
  '/guides/fastest-rising-house-prices/',
  '/guides/safest-areas-england-wales/',
  '/tools/',
  '/methodology/',
  '/about/',
];

export const GET: APIRoute = () => {
  const countySlugs = getAllCountySlugs();

  const staticUrls = staticGuides.map(path => `  <url>
    <loc>${SITE}${path}</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.6</priority>
  </url>`);

  const countyGuideUrls = countySlugs.map(slug => `  <url>
    <loc>${SITE}/guides/${slug}-house-prices/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.6</priority>
  </url>`);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${staticUrls.join('\n')}
${countyGuideUrls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
