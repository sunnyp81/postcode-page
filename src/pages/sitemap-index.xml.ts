import type { APIRoute } from 'astro';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

const sitemaps = [
  { loc: `${SITE}/sitemap-postcodes.xml` },
  { loc: `${SITE}/sitemap-counties.xml` },
  { loc: `${SITE}/sitemap-guides.xml` },
  { loc: `${SITE}/sitemap-blog.xml` },
];

export const GET: APIRoute = () => {
  const entries = sitemaps.map(s => `  <sitemap>
    <loc>${s.loc}</loc>
    <lastmod>${TODAY}</lastmod>
  </sitemap>`);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${entries.join('\n')}
</sitemapindex>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
