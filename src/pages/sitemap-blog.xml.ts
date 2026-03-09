import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';

const SITE = 'https://postcode.page';

export const GET: APIRoute = async () => {
  const posts = await getCollection('blog');
  const urls = posts.map(p => {
    const slug = p.id.replace(/\.mdx?$/, '');
    const lastmod = p.data.updatedDate || p.data.pubDate;
    return `  <url>
    <loc>${SITE}/blog/${slug}/</loc>
    <lastmod>${lastmod}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>`;
  });

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
