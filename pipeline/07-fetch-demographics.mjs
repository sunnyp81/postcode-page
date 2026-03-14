/**
 * Step 7: Fetch ONS demographics at Local Authority level and map to outcodes.
 * Data: mid-year population estimates + Census 2021 tenure/age data.
 *
 * Run: node pipeline/07-fetch-demographics.mjs
 * Output: pipeline/processed/demographics-by-outcode.json
 */

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROCESSED_DIR = join(__dirname, 'processed');
const RAW_DIR = join(__dirname, 'raw');
mkdirSync(PROCESSED_DIR, { recursive: true });
mkdirSync(RAW_DIR, { recursive: true });

console.log('\n=== Step 7: Fetch ONS Demographics ===\n');

// --- 1. Population by LA from NOMIS mid-year estimates (latest = 2023) ---
console.log('  Fetching population by local authority...');
const popUrl = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_31_1/data.csv?date=latest&geography=TYPE464&sex=7&age=0&measures=20100&select=geography_code,geography_name,obs_value';
const popRes = await fetch(popUrl, { signal: AbortSignal.timeout(30000) });
if (!popRes.ok) { console.error('  ✗ Population fetch failed:', popRes.status); process.exit(1); }
const popCsv = await popRes.text();
const popLines = popCsv.trim().split('\n');
const laPopulation = {}; // la_name → population
for (const line of popLines.slice(1)) {
  const [code, name, pop] = line.split(',').map(s => s.replace(/"/g, '').trim());
  if (name && pop) laPopulation[name] = parseInt(pop) || 0;
}
console.log(`  ✓ Population data for ${Object.keys(laPopulation).length} LAs`);

// Median age: skipped (no reliable NOMIS bulk endpoint found)
// Will use synthetic fallback in aggregate step
const laMedianAge = {};

// --- 3. Census 2021 tenure (owner-occupied vs rented) at LA level ---
// NM_2072_1: range syntax works for E06/E08/E09 but NOT E07 (shire districts).
// Must use explicit code list for E07.
console.log('  Fetching LA codes for tenure lookup...');
const popCsvRaw = await (await fetch(
  'https://www.nomisweb.co.uk/api/v01/dataset/NM_31_1/data.csv?date=latest&geography=TYPE464&sex=7&age=0&measures=20100&select=geography_code,geography_name',
  { signal: AbortSignal.timeout(30000) }
)).text();
const e07CodesArr = [];
const otherCodesArr = [];
for (const line of popCsvRaw.trim().split('\n').slice(1)) {
  const parts = line.split(',').map(s => s.replace(/"/g, '').trim());
  if (!parts[0].startsWith('E')) continue;
  if (parts[0].startsWith('E07')) e07CodesArr.push(parts[0]);
  else otherCodesArr.push(parts[0]);
}

console.log('  Fetching tenure data from Census 2021...');
const tenureBase = 'https://www.nomisweb.co.uk/api/v01/dataset/NM_2072_1/data.csv?date=latest&c2021_tenure_9=1,2,4,5,6&measures=20301&select=geography_code,geography_name,c2021_tenure_9_name,obs_value';
const [tenureRes1, tenureRes2] = await Promise.all([
  fetch(`${tenureBase}&geography=${otherCodesArr.join(',')}`, { signal: AbortSignal.timeout(60000) }),
  fetch(`${tenureBase}&geography=${e07CodesArr.join(',')}`, { signal: AbortSignal.timeout(60000) }),
]);
const tenureRes = { ok: tenureRes1.ok || tenureRes2.ok };
const tenureCsv1 = tenureRes1.ok ? await tenureRes1.text() : '';
const tenureCsv2 = tenureRes2.ok ? await tenureRes2.text() : '';
const tenureText = tenureCsv1 + '\n' + (tenureCsv2 ? tenureCsv2.split('\n').slice(1).join('\n') : '');

const laTenure = {}; // la_name → { ownerOccupied, privateRented, socialRented }
if (tenureRes.ok) {
  const csv = tenureText;
  const lines = csv.trim().split('\n');
  for (const line of lines.slice(1)) {
    const parts = line.split(',').map(s => s.replace(/"/g, '').trim());
    if (parts.length < 4) continue;
    const name = parts[1];
    const tenureType = parts[2].toLowerCase();
    const pct = parseFloat(parts[3]);
    if (!name || isNaN(pct)) continue;
    if (!laTenure[name]) laTenure[name] = {};
    if (tenureType.includes('owned outright') || tenureType.includes('mortgage')) {
      laTenure[name].ownerOccupied = (laTenure[name].ownerOccupied || 0) + pct;
    } else if (tenureType.includes('private rented')) {
      laTenure[name].privateRented = (laTenure[name].privateRented || 0) + pct;
    } else if (tenureType.includes('social rented') || tenureType.includes('council')) {
      laTenure[name].socialRented = (laTenure[name].socialRented || 0) + pct;
    }
  }
  console.log(`  ✓ Tenure data for ${Object.keys(laTenure).length} LAs`);
} else {
  console.log('  ⚠ Tenure fetch failed:', tenureRes.status);
}

// --- 4. Load outcode→LA mapping from council tax step ---
const ctCachePath = join(RAW_DIR, 'outcode-la-cache.json');
if (!existsSync(ctCachePath)) {
  console.error('  ✗ outcode-la-cache.json not found — run step 06 first');
  process.exit(1);
}
const outcodeLaMap = JSON.parse(readFileSync(ctCachePath, 'utf8'));
console.log(`\n  Using outcode→LA mapping for ${Object.keys(outcodeLaMap).length} outcodes`);

// --- 5. Build output ---
const output = {};
let mapped = 0;
let missing = 0;

for (const [outcode, la] of Object.entries(outcodeLaMap)) {
  if (!la) { missing++; continue; }

  const pop = laPopulation[la];
  const medAge = laMedianAge[la];
  const tenure = laTenure[la];

  if (!pop && !medAge && !tenure) { missing++; continue; }

  output[outcode] = {
    localAuthority: la,
    population: pop || null,
    medianAge: medAge ? Math.round(medAge * 10) / 10 : null,
    ownerOccupiedPct: tenure?.ownerOccupied ? Math.round(tenure.ownerOccupied) : null,
    privateRentedPct: tenure?.privateRented ? Math.round(tenure.privateRented) : null,
    socialRentedPct: tenure?.socialRented ? Math.round(tenure.socialRented) : null,
  };
  mapped++;
}

console.log(`  ✓ ${mapped} outcodes with demographics | ${missing} missing`);

const outPath = join(PROCESSED_DIR, 'demographics-by-outcode.json');
writeFileSync(outPath, JSON.stringify(output, null, 2));
console.log('  Saved to pipeline/processed/demographics-by-outcode.json');
console.log('  Next: run node pipeline/09-aggregate.mjs\n');
