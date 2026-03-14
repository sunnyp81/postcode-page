/**
 * pipeline/11b-fetch-median-age.mjs
 * Fetch mid-year population estimates by 5-year age band from NOMIS NM_31_1
 * Compute median age per local authority, then map to outcodes
 */
import { writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = join(__dirname, 'processed', 'median-age-by-outcode.json');
const LA_AGE_CACHE = join(__dirname, 'raw', 'la-age-bands.csv');
const OUTCODE_LA_CACHE = join(__dirname, 'raw', 'outcode-la-cache.json');
const OUTCODE_LA_CODE_CACHE = join(__dirname, 'raw', 'outcode-la-code-cache.json');

// Age band codes in NM_31_1 (1=under 1, 2=1-4, 3=5-9, ... 20=85+)
// Midpoints for median estimation
const AGE_BAND_MID = [0.5, 2.5, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57, 62, 67, 72, 77, 82, 87, 90];
const AGE_CODES = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20';

// Step 1: Fetch age band data from NOMIS
async function fetchAgeData() {
  if (existsSync(LA_AGE_CACHE)) {
    console.log('Using cached age band data');
    return readFileSync(LA_AGE_CACHE, 'utf-8');
  }
  const url = `https://www.nomisweb.co.uk/api/v01/dataset/NM_31_1/data.csv?geography=TYPE464&sex=7&measures=20100&age=${AGE_CODES}&date=latestMINUS1&select=GEOGRAPHY_CODE,AGE_CODE,OBS_VALUE`;
  console.log('Fetching age band data from NOMIS...');
  const res = await fetch(url);
  if (!res.ok) throw new Error(`NOMIS HTTP ${res.status}`);
  const csv = await res.text();
  writeFileSync(LA_AGE_CACHE, csv);
  console.log(`Cached ${csv.split('\n').length - 1} rows`);
  return csv;
}

// Step 2: Parse CSV and compute median per LA
function computeMedianByLA(csv) {
  const lines = csv.trim().split('\n');
  const header = lines[0].split(',').map(h => h.replace(/"/g, '').toLowerCase());
  const geoIdx = header.indexOf('geography_code');
  const ageIdx = header.indexOf('age_code');
  const valIdx = header.indexOf('obs_value');

  if (geoIdx < 0 || ageIdx < 0 || valIdx < 0) {
    console.error('Columns not found. Header:', header);
    return {};
  }

  // Accumulate population by LA → age band
  const byLA = new Map();
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',').map(v => v.replace(/"/g, '').trim());
    const geo = cols[geoIdx];
    const ageCode = parseInt(cols[ageIdx]);
    const val = parseInt(cols[valIdx]);
    if (!geo || isNaN(ageCode) || isNaN(val) || ageCode < 1 || ageCode > 20) continue;

    if (!byLA.has(geo)) byLA.set(geo, new Array(20).fill(0));
    byLA.get(geo)[ageCode - 1] = val;
  }

  // Compute median age for each LA
  const result = {};
  for (const [geo, bands] of byLA) {
    const total = bands.reduce((s, v) => s + v, 0);
    if (!total) continue;
    let cumulative = 0;
    const half = total / 2;
    for (let i = 0; i < bands.length; i++) {
      cumulative += bands[i];
      if (cumulative >= half) {
        result[geo] = Math.round(AGE_BAND_MID[i]);
        break;
      }
    }
  }
  return result;
}

// Step 3: Build outcode → LA code mapping using postcodes.io GET
async function buildOutcodeToLACode() {
  if (existsSync(OUTCODE_LA_CODE_CACHE)) {
    const data = JSON.parse(readFileSync(OUTCODE_LA_CODE_CACHE, 'utf-8'));
    if (Object.keys(data).length > 100) {
      console.log(`Loaded ${Object.keys(data).length} outcode→LA codes from cache`);
      return data;
    }
  }

  const outcodeLA = JSON.parse(readFileSync(OUTCODE_LA_CACHE, 'utf-8'));
  const outcodes = Object.keys(outcodeLA);
  const laCodeMap = {};

  console.log(`Fetching LA codes for ${outcodes.length} outcodes from postcodes.io...`);
  let done = 0;
  const CONCURRENCY = 20;

  for (let i = 0; i < outcodes.length; i += CONCURRENCY) {
    const batch = outcodes.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (outcode) => {
      try {
        const res = await fetch(`https://api.postcodes.io/outcodes/${encodeURIComponent(outcode)}`);
        if (!res.ok) return;
        const json = await res.json();
        const codes = json?.result?.admin_district_code;
        if (codes && codes.length > 0) {
          laCodeMap[outcode.toUpperCase()] = codes[0];
        }
      } catch {}
    }));
    done += batch.length;
    if (done % 200 === 0) process.stdout.write(`${done}/${outcodes.length} `);
    await new Promise(r => setTimeout(r, 50));
  }
  console.log(`\nFetched ${Object.keys(laCodeMap).length} outcode→LA codes`);
  writeFileSync(OUTCODE_LA_CODE_CACHE, JSON.stringify(laCodeMap));
  return laCodeMap;
}

// Main
const csv = await fetchAgeData();
const medianByLA = computeMedianByLA(csv);
console.log(`Computed median age for ${Object.keys(medianByLA).length} LAs`);
console.log('Sample:', Object.entries(medianByLA).slice(0, 3));

const outcodeToLACode = await buildOutcodeToLACode();

// Map outcode → median age
const result = {};
let matched = 0;
for (const [outcode, laCode] of Object.entries(outcodeToLACode)) {
  if (medianByLA[laCode]) {
    result[outcode] = medianByLA[laCode];
    matched++;
  }
}

console.log(`Matched ${matched} outcodes to real median age`);
writeFileSync(OUT, JSON.stringify(result));
console.log(`Written to ${OUT}`);
