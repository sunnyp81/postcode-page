// Submit URLs to IndexNow (Bing + search engines)
// Usage: node scripts/indexnow-ping.mjs
// Or pass specific URLs: node scripts/indexnow-ping.mjs /sw1a/ /sw1b/

import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SITE = 'https://postcode.page';
const KEY = 'fd0147cf4f4446f4984568ee673533e6';
const KEY_LOCATION = `${SITE}/${KEY}.txt`;
const INDEXNOW_ENDPOINT = 'https://www.bing.com/indexnow';
const BATCH_SIZE = 500;

function getPriorityUrls() {
  const staticUrls = [
    '/',
    '/guides/',
    '/guides/average-house-price-uk/',
    '/guides/best-areas-for-families/',
    '/guides/cheapest-places-to-live-england/',
    '/guides/fastest-rising-house-prices/',
    '/guides/safest-areas-england-wales/',
    '/blog/',
    '/tools/',
    '/tools/affordability/',
    '/tools/compare/',
    '/tools/stamp-duty/',
    '/methodology/',
    '/about/',
    '/regions/',
  ];

  // Add postcode pages
  const postcodeDir = join(__dirname, '..', 'data', 'postcodes');
  const postcodeSlugs = readdirSync(postcodeDir)
    .map(f => f.replace('.json', ''))
    .map(slug => `/${slug}/`);

  // Add region/county pages
  const countiesDir = join(__dirname, '..', 'data', 'counties');
  const countySlugs = readdirSync(countiesDir)
    .map(f => f.replace('.json', ''))
    .map(slug => `/counties/${slug}/`);

  return [...staticUrls, ...postcodeSlugs, ...countySlugs];
}

async function submitBatch(urls) {
  const body = {
    host: 'postcode.page',
    key: KEY,
    keyLocation: KEY_LOCATION,
    urlList: urls.map(u => u.startsWith('http') ? u : `${SITE}${u}`),
  };

  const res = await fetch(INDEXNOW_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  });

  return res.status;
}

async function main() {
  const customUrls = process.argv.slice(2);
  const urls = customUrls.length > 0 ? customUrls : getPriorityUrls();

  console.log(`Submitting ${urls.length} URLs to IndexNow...`);

  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batch = urls.slice(i, i + BATCH_SIZE);
    const status = await submitBatch(batch);
    console.log(`Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} URLs → HTTP ${status}`);
    if (status !== 200 && status !== 202) {
      console.error(`Unexpected status ${status} — check key file is live at ${KEY_LOCATION}`);
    }
  }

  console.log('Done.');
}

main().catch(console.error);
