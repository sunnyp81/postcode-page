/**
 * Step 6: Extract per-LA Band D council tax rates from MHCLG 2025-26 data.
 * Maps local authorities to postcode outcodes via postcodes.io.
 *
 * Run: node pipeline/06-fetch-council-tax.mjs
 * Output: pipeline/processed/council-tax-by-outcode.json
 *
 * Source: MHCLG Band D Council Tax Live Tables 2025-26
 */

import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const PROCESSED_DIR = join(__dirname, 'processed');
mkdirSync(RAW_DIR, { recursive: true });
mkdirSync(PROCESSED_DIR, { recursive: true });

const ODS_FILE = join(RAW_DIR, 'band-d-council-tax-2025.ods');
const ODS_URL = 'https://assets.publishing.service.gov.uk/media/680a3ca79b25e1a97c9d8471/Band_D_2025-26.ods';

console.log('\n=== Step 6: Extract Council Tax Band D Rates ===\n');

// Download if not present
if (!existsSync(ODS_FILE)) {
  console.log('  Downloading MHCLG Band D Council Tax 2025-26...');
  const res = await fetch(ODS_URL, {
    headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) { console.error(`  ✗ Download failed: ${res.status}`); process.exit(1); }
  const buf = await res.arrayBuffer();
  writeFileSync(ODS_FILE, Buffer.from(buf));
  console.log(`  ✓ Downloaded ${(buf.byteLength / 1024).toFixed(0)}KB`);
} else {
  console.log('  ✓ Using cached ODS file');
}

// Extract content.xml from ODS (which is a ZIP)
const contentXmlPath = join(RAW_DIR, 'ct-content.xml');
if (!existsSync(contentXmlPath)) {
  console.log('  Extracting content.xml...');
  execSync(`unzip -p "${ODS_FILE}" content.xml > "${contentXmlPath}"`, { stdio: 'pipe', shell: true });
  console.log('  ✓ Extracted');
} else {
  console.log('  ✓ Using cached content.xml');
}

const xml = readFileSync(contentXmlPath, 'utf8');

// Use Area_CT sheet: total Band D area council tax (includes county, police, fire precepts)
// Sheet bounds: pos 11129058 to 13415603
// Structure: [hist_code, ons_code, authority, current, class, region, 1993-94..., 2025-26, notes]
// Col indices: 2=authority, 3=current, 36=2025-26
const sheetXml = xml.slice(11129058, 13415603);
const rows = sheetXml.match(/<table:table-row\b[\s\S]*?<\/table:table-row>/g) || [];

console.log(`  Found ${rows.length} rows in Area_CT sheet`);

const laRates = {}; // la_name → bandD
let parsed = 0;

for (const row of rows.slice(3)) {
  const cells = [...row.matchAll(/<text:p[^>]*>([^<]*)<\/text:p>/g)].map(m => m[1].trim());
  if (cells.length < 10) continue;
  const laName = cells[2];
  const isCurrent = cells[3];
  const rawBandD = cells[36]; // 2025-26 column

  if (!laName || isCurrent !== 'YES') continue;
  if (!rawBandD || rawBandD === '[z]' || rawBandD === '[x]') continue;

  const bandD = parseFloat(rawBandD.replace(/,/g, ''));
  if (isNaN(bandD) || bandD < 500 || bandD > 6000) continue;

  laRates[laName] = bandD;
  parsed++;
}

console.log(`  ✓ Parsed ${parsed} current LAs`);
console.log('  Sample: ' + Object.entries(laRates).slice(0, 3).map(([k,v]) => `${k}=£${v}`).join(', '));

// Get all outcodes from processed LR data
const lrPath = join(PROCESSED_DIR, 'lr-by-outcode.json');
if (!existsSync(lrPath)) {
  console.error('  ✗ house-prices-by-outcode.json not found — run step 02 first');
  process.exit(1);
}
const lrData = JSON.parse(readFileSync(lrPath, 'utf8'));
const allOutcodes = Object.keys(lrData);
console.log(`\n  Looking up LA for ${allOutcodes.length} outcodes via postcodes.io...`);

// Fetch LA per outcode from postcodes.io — single endpoint per outcode
const outcodeLaMap = {}; // outcode → la_name
const CACHE_FILE = join(RAW_DIR, 'outcode-la-cache.json');

if (existsSync(CACHE_FILE)) {
  Object.assign(outcodeLaMap, JSON.parse(readFileSync(CACHE_FILE, 'utf8')));
  console.log(`  Loaded ${Object.keys(outcodeLaMap).length} cached LA lookups`);
}

const toFetch = allOutcodes.filter(o => !outcodeLaMap[o]);
console.log(`  Need to fetch ${toFetch.length} outcodes`);

// Batch in groups of 50 with small delay
let fetched = 0;
for (let i = 0; i < toFetch.length; i++) {
  const outcode = toFetch[i];
  try {
    const res = await fetch(`https://api.postcodes.io/outcodes/${outcode}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (res.ok) {
      const json = await res.json();
      const la = json.result?.admin_district?.[0] || json.result?.admin_county?.[0];
      outcodeLaMap[outcode] = la || null;
    } else {
      outcodeLaMap[outcode] = null;
    }
  } catch {
    outcodeLaMap[outcode] = null;
  }
  fetched++;
  if (fetched % 100 === 0) {
    process.stdout.write(`\r    ${fetched}/${toFetch.length}...`);
    writeFileSync(CACHE_FILE, JSON.stringify(outcodeLaMap));
    await new Promise(r => setTimeout(r, 50));
  }
}
if (toFetch.length > 0) {
  console.log(`\n  ✓ Fetched ${fetched} LA lookups`);
  writeFileSync(CACHE_FILE, JSON.stringify(outcodeLaMap));
}

// Build outcode → council tax
const output = {};
let mapped = 0;
let missing = 0;

for (const outcode of allOutcodes) {
  const la = outcodeLaMap[outcode];
  if (!la) { missing++; continue; }
  const bandD = laRates[la];
  if (!bandD) { missing++; continue; }
  output[outcode] = { bandD, localAuthority: la };
  mapped++;
}

console.log(`\n  ✓ Mapped ${mapped} outcodes | Missing: ${missing}`);

const outPath = join(PROCESSED_DIR, 'council-tax-by-outcode.json');
writeFileSync(outPath, JSON.stringify(output, null, 2));
console.log('  Saved to pipeline/processed/council-tax-by-outcode.json');
console.log('  Next: run node pipeline/09-aggregate.mjs\n');
