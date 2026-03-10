import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';

const SITE = 'https://postcode.page';
const TODAY = new Date().toISOString().split('T')[0];

export const GET: APIRoute = async () => {
  const posts = await getCollection('blog');
  const categories = [...new Set(posts.map(p => p.data.category))];

  const postUrls = posts.map(p => {
    const slug = p.id.replace(/\.mdx?$/, '');
    const lastmod = p.data.updatedDate || p.data.pubDate;
    return `  <url>
    <loc>${SITE}/blog/${slug}/</loc>
    <lastmod>${lastmod}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>`;
  });

  const categoryUrls = categories.map(cat => {
    const slug = cat.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '');
    return `  <url>
    <loc>${SITE}/blog/category/${slug}/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.5</priority>
  </url>`;
  });

  const blogIndex = `  <url>
    <loc>${SITE}/blog/</loc>
    <lastmod>${TODAY}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.6</priority>
  </url>`;

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${blogIndex}
${categoryUrls.join('\n')}
${postUrls.join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8' },
  });
};
