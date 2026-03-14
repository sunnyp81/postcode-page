/**
 * pipeline/11-fetch-median-age.mjs
 * Fetch Census 2021 age group data from NOMIS and compute real median age per LA
 * Then map LA → outcode using outcode-la-cache.json
 */
import { writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CACHE = join(__dirname, 'raw', 'outcode-la-cache.json');
const OUT = join(__dirname, 'processed', 'median-age-by-outcode.json');

// NOMIS Census 2021 TS007A — Population by 5-year age groups
// Dataset NM_2025_1 is TS007 (Age by single year) — too many rows
// Use NM_2082_1 = TS009 (Age by 5-year bands) or discover dynamically
// TS007 = 5-year age bands dataset
// We'll try a few candidate IDs

const NOMIS_BASE = 'https://www.nomisweb.co.uk/api/v01/dataset';

async function discoverAgeDataset() {
  // Try known IDs for Census 2021 age band data
  // NM_2072_1 = TS054 (tenure), TS007 is likely NM_2025_1 area
  // Let's use the search API
  const url = `${NOMIS_BASE}/def.sdmx.json?search=TS007`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    const datasets = json?.structure?.keyfamilies?.keyfamily || [];
    for (const ds of datasets) {
      if (ds.id && ds.name?.value?.includes('age')) {
        console.log('Found dataset:', ds.id, ds.name.value);
        return ds.id;
      }
    }
  } catch (e) {
    console.log('Search failed, trying known IDs...');
  }
  return null;
}

// Age band midpoints for computing median (Census 2021 5-year bands)
// Bands: 0-4, 5-9, 10-14, ..., 85-89, 90+
const AGE_BANDS = [
  { label: '0-4', mid: 2, nomisCode: 1 },
  { label: '5-9', mid: 7, nomisCode: 2 },
  { label: '10-14', mid: 12, nomisCode: 3 },
  { label: '15-19', mid: 17, nomisCode: 4 },
  { label: '20-24', mid: 22, nomisCode: 5 },
  { label: '25-29', mid: 27, nomisCode: 6 },
  { label: '30-34', mid: 32, nomisCode: 7 },
  { label: '35-39', mid: 37, nomisCode: 8 },
  { label: '40-44', mid: 42, nomisCode: 9 },
  { label: '45-49', mid: 47, nomisCode: 10 },
  { label: '50-54', mid: 52, nomisCode: 11 },
  { label: '55-59', mid: 57, nomisCode: 12 },
  { label: '60-64', mid: 62, nomisCode: 13 },
  { label: '65-69', mid: 67, nomisCode: 14 },
  { label: '70-74', mid: 72, nomisCode: 15 },
  { label: '75-79', mid: 77, nomisCode: 16 },
  { label: '80-84', mid: 82, nomisCode: 17 },
  { label: '85-89', mid: 87, nomisCode: 18 },
  { label: '90+', mid: 92, nomisCode: 19 },
];

function computeMedianAge(bands) {
  // bands = array of { mid, count } in order
  const total = bands.reduce((s, b) => s + b.count, 0);
  if (!total) return null;
  let cumulative = 0;
  const half = total / 2;
  for (const band of bands) {
    cumulative += band.count;
    if (cumulative >= half) return band.mid;
  }
  return null;
}

async function fetchAgeData(datasetId) {
  // Fetch all LAs at once using TYPE464
  const url = `${NOMIS_BASE}/${datasetId}/data.csv?geography=TYPE464&measures=20100&select=geography_code,c_age_name,obs_value&uid=`;
  console.log(`Fetching from: ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

// Parse CSV and compute median per LA
function parseAndCompute(csv) {
  const lines = csv.split('\n');
  const header = lines[0].toLowerCase().split(',');
  const geoIdx = header.findIndex(h => h.includes('geography_code'));
  const ageIdx = header.findIndex(h => h.includes('c_age_name') || h.includes('age'));
  const valIdx = header.findIndex(h => h.includes('obs_value') || h.includes('value'));

  if (geoIdx < 0 || ageIdx < 0 || valIdx < 0) {
    console.error('Could not find required columns. Header:', header);
    return {};
  }

  // Accumulate counts by LA → age band
  const byLA = new Map();
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < Math.max(geoIdx, ageIdx, valIdx) + 1) continue;
    const geo = cols[geoIdx]?.trim();
    const ageName = cols[ageIdx]?.trim();
    const val = parseInt(cols[valIdx]?.trim());
    if (!geo || !ageName || isNaN(val)) continue;

    if (!byLA.has(geo)) byLA.set(geo, {});
    const la = byLA.get(geo);
    la[ageName] = (la[ageName] || 0) + val;
  }

  // Compute median per LA
  const result = {};
  for (const [geo, ageCounts] of byLA) {
    // Build ordered bands array
    const bands = [];
    for (const band of AGE_BANDS) {
      // Try exact match, partial match
      const key = Object.keys(ageCounts).find(k =>
        k.includes(band.label) || k === String(band.nomisCode) || k === band.label
      );
      bands.push({ mid: band.mid, count: key ? ageCounts[key] : 0 });
    }
    const median = computeMedianAge(bands);
    if (median) result[geo] = median;
  }
  return result;
}

// Main
const outcodeLA = JSON.parse(readFileSync(CACHE, 'utf-8'));

// Try dataset IDs for Census 2021 age groups
// TS007 5-year age bands — try several candidate NM IDs
const candidateIds = ['NM_2010_1', 'NM_2025_1', 'NM_2082_1', 'NM_2065_1'];
let ageByLA = {};

for (const id of candidateIds) {
  try {
    console.log(`Trying dataset ${id}...`);
    const csv = await fetchAgeData(id);
    if (csv.includes('geography_code') || csv.includes('GEOGRAPHY_CODE')) {
      console.log(`Dataset ${id} returned data`);
      ageByLA = parseAndCompute(csv);
      if (Object.keys(ageByLA).length > 50) {
        console.log(`Got ${Object.keys(ageByLA).length} LAs from ${id}`);
        break;
      }
    }
  } catch (e) {
    console.log(`${id} failed: ${e.message}`);
  }
}

if (Object.keys(ageByLA).length === 0) {
  // Fallback: use mid-year estimates age structure proxy
  // Use NM_31_1 with age groups if available
  console.log('All census datasets failed, trying mid-year estimates...');
  try {
    const url = `${NOMIS_BASE}/NM_31_1/data.csv?geography=TYPE464&sex=7&age=200&measures=20100&select=geography_code,obs_value`;
    const res = await fetch(url);
    const csv = await res.text();
    // This gives median age directly if age=200 is median age variable
    const lines = csv.split('\n');
    const header = lines[0].toLowerCase().split(',');
    const geoIdx = header.findIndex(h => h.includes('geography_code'));
    const valIdx = header.findIndex(h => h.includes('obs_value'));
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const geo = cols[geoIdx]?.trim();
      const val = parseFloat(cols[valIdx]?.trim());
      if (geo && !isNaN(val)) ageByLA[geo] = Math.round(val);
    }
    console.log(`Got ${Object.keys(ageByLA).length} LAs from median age variable`);
  } catch (e) {
    console.log('Fallback also failed:', e.message);
  }
}

// Map LA code → outcode → median age
const result = {};
for (const [outcode, laName] of Object.entries(outcodeLA)) {
  // Find matching LA code by name
  const laCode = Object.keys(ageByLA).find(code => {
    // We need LA code for the outcode — we have LA name from cache
    // Cross-reference: the cache has LA name, we need to match to ONS code
    // This won't work directly without a name→code lookup
    return false;
  });
}

// Better: we need to use the LA code from the demographics pipeline
// Load demographics to get the LA→outcode mapping with codes
const demoPath = join(__dirname, 'processed', 'demographics-by-outcode.json');
if (existsSync(demoPath)) {
  const demo = JSON.parse(readFileSync(demoPath, 'utf-8'));
  // Demo has outcode → LA name. We need LA code from ageByLA.
  // ageByLA keys are ONS codes like E07000098
  // We need a name→code mapping. Build from scratch using postcodes.io bulk LA lookup.

  // Actually: re-use the outcode-la-cache but we need ONS codes, not names.
  // postcodes.io /outcodes/{outcode} returns admin_district_code

  console.log('Building LA code lookup from postcodes.io...');
  const laCodeCache = join(__dirname, 'raw', 'outcode-la-code-cache.json');
  let laCodeMap = {};

  if (existsSync(laCodeCache)) {
    laCodeMap = JSON.parse(readFileSync(laCodeCache, 'utf-8'));
    console.log(`Loaded ${Object.keys(laCodeMap).length} entries from cache`);
  } else {
    // Fetch outcode → LA code for all outcodes
    const outcodes = Object.keys(outcodeLA);
    const BATCH = 50;
    for (let i = 0; i < outcodes.length; i += BATCH) {
      const batch = outcodes.slice(i, i + BATCH);
      try {
        const res = await fetch('https://api.postcodes.io/outcodes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ outcodes: batch }),
        });
        const json = await res.json();
        if (json.result) {
          for (const item of json.result) {
            if (item?.result?.admin_district_code) {
              laCodeMap[item.query] = item.result.admin_district_code;
            }
          }
        }
      } catch (e) {}
      if (i % 500 === 0) console.log(`Processed ${i}/${outcodes.length}`);
      await new Promise(r => setTimeout(r, 100));
    }
    writeFileSync(laCodeCache, JSON.stringify(laCodeMap));
    console.log(`Saved ${Object.keys(laCodeMap).length} outcode→LA codes`);
  }

  // Now map: outcode → LA code → median age
  let matched = 0;
  for (const [outcode, laCode] of Object.entries(laCodeMap)) {
    if (ageByLA[laCode]) {
      result[outcode] = ageByLA[laCode];
      matched++;
    }
  }
  console.log(`Matched ${matched} outcodes to median age`);
}

writeFileSync(OUT, JSON.stringify(result));
console.log(`Written ${Object.keys(result).length} outcode median ages to ${OUT}`);
