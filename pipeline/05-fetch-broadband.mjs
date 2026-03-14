/**
 * Step 5: Download Ofcom Connected Nations broadband coverage data.
 * Produces per-outcode averages for: superfast %, ultrafast %, gigabit %
 * and estimates average download speed from coverage tiers.
 *
 * Run: node pipeline/05-fetch-broadband.mjs
 * Output: pipeline/processed/broadband-by-outcode.json
 *
 * Source: Ofcom Connected Nations 2024 (July 2024 data)
 */

import { existsSync, writeFileSync, mkdirSync, readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const PROCESSED_DIR = join(__dirname, 'processed');
const OFCOM_DIR = join(RAW_DIR, 'ofcom-broadband');
mkdirSync(RAW_DIR, { recursive: true });
mkdirSync(PROCESSED_DIR, { recursive: true });
mkdirSync(OFCOM_DIR, { recursive: true });

const ZIP_URL = 'https://www.ofcom.org.uk/siteassets/resources/documents/research-and-data/multi-sector/infrastructure-research/connected-nations-2024/data-downloads/202407-fixed-coverage-postcodes-r01.zip';
const ZIP_FILE = join(RAW_DIR, 'ofcom-broadband-postcodes.zip');

console.log('\n=== Step 5: Fetch Ofcom Broadband Coverage Data ===\n');

// Download ZIP
if (!existsSync(ZIP_FILE)) {
  console.log('  Downloading Ofcom Connected Nations 2024 (Jul 2024)...');
  const res = await fetch(ZIP_URL, {
    headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
    signal: AbortSignal.timeout(120000),
  });
  if (!res.ok) { console.error(`  ✗ Download failed: ${res.status}`); process.exit(1); }
  const buf = await res.arrayBuffer();
  writeFileSync(ZIP_FILE, Buffer.from(buf));
  console.log(`  ✓ Downloaded ${(buf.byteLength / 1024 / 1024).toFixed(0)}MB`);
} else {
  console.log('  ✓ Using cached ZIP');
}

// Extract outer ZIP, then inner ZIP
const OUTER_DIR = join(OFCOM_DIR, '202407_fixed_postcode_coverage_r01');
const INNER_ZIP = join(OUTER_DIR, 'cn202407_postcode_files.zip');
const CSV_DIR = join(OUTER_DIR, 'postcode_files', 'postcode_files');

if (!existsSync(CSV_DIR)) {
  console.log('  Extracting outer ZIP...');
  try {
    execSync(`unzip -o "${ZIP_FILE}" -d "${OFCOM_DIR}"`, { stdio: 'pipe' });
    console.log('  Extracting inner ZIP...');
    execSync(`unzip -o "${INNER_ZIP}" -d "${join(OUTER_DIR, 'postcode_files')}"`, { stdio: 'pipe' });
    console.log('  ✓ Extracted');
  } catch (e) {
    console.error('  ✗ Unzip failed:', e.message);
    process.exit(1);
  }
} else {
  console.log('  ✓ Using cached extracted files');
}

import { readdirSync } from 'node:fs';
const csvFiles = readdirSync(CSV_DIR)
  .filter(f => f.endsWith('.csv'))
  .map(f => join(CSV_DIR, f));
console.log(`  Found ${csvFiles.length} CSV files`);
if (csvFiles.length === 0) { console.error('  ✗ No CSVs found after extraction'); process.exit(1); }

// Parse CSV header to find columns
function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (const ch of line) {
    if (ch === '"') inQ = !inQ;
    else if (ch === ',' && !inQ) { result.push(cur.trim()); cur = ''; }
    else cur += ch;
  }
  result.push(cur.trim()); return result;
}

// Read first CSV to get headers
const firstCSV = readFileSync(csvFiles[0], 'utf-8');
const firstLines = firstCSV.split('\n');
const headers = parseCSVLine(firstLines[0]).map(h => h.toLowerCase().replace(/[^a-z0-9]/g, '_'));
console.log('  Headers:', headers.slice(0, 10).join(', '));

// Find key column indices
// Actual headers: postcode, postcode_space, postcode area, ..., SFBB availability (% premises), UFBB (100Mbit/s) availability, ..., Gigabit availability
const colPostcode = 0; // no-space postcode e.g. GU102QB
const colSFBB = headers.findIndex(h => h.includes('sfbb') && h.includes('avail'));
const colUFBB = headers.findIndex(h => h.includes('ufbb') && h.includes('100') && h.includes('avail'))
  || headers.findIndex(h => h.includes('ufbb') && h.includes('avail'));
const colGigabit = headers.findIndex(h => h.includes('gigabit') && h.includes('avail'));
const colFTTP = headers.findIndex(h => h.includes('fttp') || h.includes('full_fibre') || h.includes('fullfibre'));

console.log(`  Columns: postcode=${colPostcode}, sfbb=${colSFBB}, ufbb=${colUFBB}, gigabit=${colGigabit}, fttp=${colFTTP}`);

// Aggregate by outcode
const byOutcode = {};  // outcode → { sfbb: [], ufbb: [], gigabit: [], fttp: [], count: 0 }
let totalPostcodes = 0;

for (const csvFile of csvFiles) {
  const content = readFileSync(csvFile, 'utf-8');
  const lines = content.split('\n');

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const cols = line.split(',');
    if (cols.length < 4) continue;

    // Postcode is without spaces: GU102QB — extract outcode by removing last 3 chars (incode is always 3)
    const rawPostcode = cols[colPostcode]?.trim().toUpperCase();
    if (!rawPostcode || rawPostcode.length < 5) continue;
    const outcode = rawPostcode.slice(0, rawPostcode.length - 3);
    if (!outcode || outcode.length < 2) continue;

    const sfbb = parseFloat(cols[colSFBB]) || 0;
    const ufbb = parseFloat(cols[colUFBB]) || 0;
    const gigabit = colGigabit >= 0 ? (parseFloat(cols[colGigabit]) || 0) : 0;
    const fttp = colFTTP >= 0 ? (parseFloat(cols[colFTTP]) || 0) : 0;

    if (!byOutcode[outcode]) {
      byOutcode[outcode] = { sfbb: 0, ufbb: 0, gigabit: 0, fttp: 0, count: 0 };
    }
    byOutcode[outcode].sfbb += sfbb;
    byOutcode[outcode].ufbb += ufbb;
    byOutcode[outcode].gigabit += gigabit;
    byOutcode[outcode].fttp += fttp;
    byOutcode[outcode].count++;
    totalPostcodes++;
  }

  process.stdout.write(`\r  Processed ${csvFiles.indexOf(csvFile) + 1}/${csvFiles.length} files | ${Object.keys(byOutcode).length} outcodes`);
}
console.log(`\n  Total postcode units: ${totalPostcodes.toLocaleString()}`);

// Compute averages and estimate download speed
// Speed estimation: gigabit coverage → ~300Mbps, UFBB → ~100Mbps, SFBB → ~50Mbps, base ~10Mbps
const output = {};
for (const [outcode, d] of Object.entries(byOutcode)) {
  if (d.count === 0) continue;
  const sfbbPct = Math.round(d.sfbb / d.count);
  const ufbbPct = Math.round(d.ufbb / d.count);
  const gigabitPct = Math.round(d.gigabit / d.count);
  const fttpPct = Math.round(d.fttp / d.count);

  // Weighted estimate of average download speed from coverage tiers
  const nonSFBBPct = 100 - sfbbPct;
  const sfbbOnlyPct = sfbbPct - ufbbPct;
  const ufbbOnlyPct = ufbbPct - gigabitPct;
  const avgDownload = Math.round(
    (nonSFBBPct / 100) * 12 +
    (sfbbOnlyPct / 100) * 55 +
    (ufbbOnlyPct / 100) * 150 +
    (gigabitPct / 100) * 400
  );

  output[outcode] = {
    avgDownload,
    superfastPct: sfbbPct,
    ultrafastPct: ufbbPct,
    gigabitPct,
    fullFibrePct: fttpPct,
    avgUpload: Math.round(avgDownload * 0.18), // typical asymmetric ratio
  };
}

const outPath = join(PROCESSED_DIR, 'broadband-by-outcode.json');
writeFileSync(outPath, JSON.stringify(output, null, 2));
console.log(`\n  ✓ ${Object.keys(output).length} outcodes with broadband data`);
console.log('  Saved to pipeline/processed/broadband-by-outcode.json');
console.log('  Next: update 09-aggregate.mjs, then npm run build\n');
