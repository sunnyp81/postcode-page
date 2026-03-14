/**
 * Step 3: Fetch crime data from Police.uk API for each outcode.
 * Uses postcodes.io to get outcode centroids, then queries Police.uk.
 *
 * Run: node pipeline/03-fetch-crime.mjs
 * Output: pipeline/processed/crime-by-outcode.json
 *
 * Rate limits: Police.uk ~15 req/s, postcodes.io generous
 * Time: ~15-20 min for 850 outcodes
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROCESSED_DIR = join(__dirname, 'processed');
const CACHE_DIR = join(__dirname, 'data', 'crime-cache');
mkdirSync(PROCESSED_DIR, { recursive: true });
mkdirSync(CACHE_DIR, { recursive: true });

const DELAY_MS = 150;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Use last 12 months of available data
// Police.uk has ~2-month lag — as of March 2026 latest is ~Jan 2026
const CRIME_MONTHS = [
  '2025-02','2025-03','2025-04','2025-05','2025-06',
  '2025-07','2025-08','2025-09','2025-10','2025-11','2025-12','2026-01',
];

// National average crimes per 1,000 residents (ONS Crime Survey 2024)
const NATIONAL_AVG_PER_1000 = 81.3;

// Category mapping → our simplified categories
const CAT_MAP = {
  'anti-social-behaviour':    'antisocialBehaviour',
  'bicycle-theft':            'otherTheft',
  'burglary':                 'burglary',
  'criminal-damage-arson':    'other',
  'drugs':                    'other',
  'other-theft':              'otherTheft',
  'possession-of-weapons':    'other',
  'public-order':             'other',
  'shoplifting':              'otherTheft',
  'theft-from-the-person':    'otherTheft',
  'vehicle-crime':            'vehicleCrime',
  'violent-crime':            'violentCrime',
  'robbery':                  'violentCrime',
  'other-crime':              'other',
};

async function fetchJSON(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
        signal: AbortSignal.timeout(20000),
      });
      if (res.status === 429) { await sleep(5000); continue; }
      if (res.status === 503 || res.status === 500) { await sleep(3000); continue; }
      if (!res.ok) return null;
      const text = await res.text();
      if (!text.trim()) return null;
      return JSON.parse(text);
    } catch {
      if (i === retries - 1) return null;
      await sleep(2000);
    }
  }
  return null;
}

// Step 1: Get all outcodes from LR processed data
const lrPath = join(PROCESSED_DIR, 'lr-by-outcode.json');
if (!existsSync(lrPath)) {
  console.error('lr-by-outcode.json not found. Run 02-parse-land-registry.mjs first.');
  process.exit(1);
}

const lrData = JSON.parse(readFileSync(lrPath, 'utf-8'));
const outcodes = Object.keys(lrData).sort();
console.log(`\n=== Step 3: Fetch Crime Data (Police.uk) ===\n`);
console.log(`  ${outcodes.length} outcodes to process`);
console.log(`  ${CRIME_MONTHS.length} months: ${CRIME_MONTHS[0]} → ${CRIME_MONTHS.at(-1)}`);

// Load existing results if resuming
const outPath = join(PROCESSED_DIR, 'crime-by-outcode.json');
const existing = existsSync(outPath) ? JSON.parse(readFileSync(outPath, 'utf-8')) : {};
const todo = outcodes.filter(oc => !existing[oc]);
console.log(`  ${Object.keys(existing).length} already done, ${todo.length} remaining\n`);

// Step 2: Get centroids from postcodes.io (bulk endpoint — 100 at a time)
const centroids = {};
const needCentroids = todo.filter(oc => !centroids[oc]);

if (needCentroids.length > 0) {
  console.log(`  Fetching ${needCentroids.length} outcode centroids from postcodes.io...`);
  const BATCH = 100;
  for (let i = 0; i < needCentroids.length; i += BATCH) {
    const batch = needCentroids.slice(i, i + BATCH);
    const res = await fetchJSON('https://api.postcodes.io/outcodes', {});
    // postcodes.io bulk outcode: POST to /outcodes with { outcodes: [...] }
    // Actually it's a GET per outcode or POST to /postcodes for full postcodes
    // Use individual GET for outcodes
    for (const oc of batch) {
      const data = await fetchJSON(`https://api.postcodes.io/outcodes/${oc.toLowerCase()}`);
      if (data?.result?.latitude) {
        centroids[oc] = { lat: data.result.latitude, lng: data.result.longitude };
      }
      await sleep(50);
    }
    const pct = Math.min(100, Math.round((i + BATCH) / needCentroids.length * 100));
    process.stdout.write(`\r  Centroids: ${pct}% (${Math.min(i + BATCH, needCentroids.length)}/${needCentroids.length})`);
  }
  console.log('\n');
}

// Step 3: Fetch crime data for each outcode
let done = 0;
const start = Date.now();

for (const outcode of todo) {
  const centroid = centroids[outcode];
  if (!centroid) {
    // No centroid found — skip
    existing[outcode] = null;
    done++;
    continue;
  }

  const { lat, lng } = centroid;
  const monthlyCounts = {};
  let totalRaw = 0;

  for (const month of CRIME_MONTHS) {
    const cacheFile = join(CACHE_DIR, `${outcode}-${month}.json`);
    let crimes;

    if (existsSync(cacheFile)) {
      crimes = JSON.parse(readFileSync(cacheFile, 'utf-8'));
    } else {
      const url = `https://data.police.uk/api/crimes-street/all-crime?lat=${lat}&lng=${lng}&date=${month}`;
      crimes = await fetchJSON(url);
      if (crimes && Array.isArray(crimes)) {
        writeFileSync(cacheFile, JSON.stringify(crimes));
      }
      await sleep(DELAY_MS);
    }

    if (!Array.isArray(crimes)) continue;

    for (const crime of crimes) {
      const cat = CAT_MAP[crime.category] ?? 'other';
      monthlyCounts[cat] = (monthlyCounts[cat] || 0) + 1;
      totalRaw++;
    }
  }

  // Annualise: monthly data × (12 / months fetched) if some months missing
  const months = CRIME_MONTHS.length;

  // Population estimate: rough 4,000 residents per outcode (will be replaced by demographics data)
  // We store raw count and compute per-1000 in aggregator
  const byType = {
    violentCrime:         Math.round((monthlyCounts.violentCrime || 0) / months * 12),
    burglary:             Math.round((monthlyCounts.burglary || 0) / months * 12),
    vehicleCrime:         Math.round((monthlyCounts.vehicleCrime || 0) / months * 12),
    antisocialBehaviour:  Math.round((monthlyCounts.antisocialBehaviour || 0) / months * 12),
    otherTheft:           Math.round((monthlyCounts.otherTheft || 0) / months * 12),
    other:                Math.round((monthlyCounts.other || 0) / months * 12),
  };

  const totalAnnual = Object.values(byType).reduce((s, v) => s + v, 0);

  existing[outcode] = {
    totalAnnual,
    byType,
    months,
    centroid: { lat, lng },
  };

  done++;

  // Save checkpoint every 25 outcodes
  if (done % 25 === 0) {
    writeFileSync(outPath, JSON.stringify(existing, null, 2));
    const elapsed = ((Date.now() - start) / 1000).toFixed(0);
    const rate = done / (Date.now() - start) * 1000 * 60;
    process.stdout.write(`\r  [${done}/${todo.length}] ${elapsed}s elapsed — ~${Math.round(rate)}/min`);
  }
}

writeFileSync(outPath, JSON.stringify(existing, null, 2));

const validOutcodes = Object.values(existing).filter(v => v !== null).length;
console.log(`\n\n  ✓ ${validOutcodes} outcodes with crime data`);
console.log(`  Saved to pipeline/processed/crime-by-outcode.json`);
console.log('\n  Next: update 09-aggregate.mjs to merge crime data, then npm run build\n');
