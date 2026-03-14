/**
 * Step 4b: Download Ofsted inspection outcomes and merge with schools data.
 * Produces: pipeline/processed/ofsted-by-urn.json
 *           pipeline/processed/schools-by-outcode.json (updated with Ofsted grades)
 *
 * Run: node pipeline/04b-fetch-ofsted.mjs
 *
 * Source: Ofsted Management Information (latest inspections), Jan 2026
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const PROCESSED_DIR = join(__dirname, 'processed');
mkdirSync(RAW_DIR, { recursive: true });
mkdirSync(PROCESSED_DIR, { recursive: true });

const OFSTED_URL = 'https://assets.publishing.service.gov.uk/media/698b20be95285e721cd7127d/Management_information_-_state-funded_schools_-_latest_inspections_as_at_31_Jan_2026.csv';
const OFSTED_RAW = join(RAW_DIR, 'ofsted-jan2026.csv');

console.log('\n=== Step 4b: Fetch & Merge Ofsted Inspection Data ===\n');

// Download if not cached
if (!existsSync(OFSTED_RAW)) {
  console.log('  Downloading Ofsted inspection outcomes (Jan 2026)...');
  const res = await fetch(OFSTED_URL, {
    headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
  });
  if (!res.ok) {
    console.error(`  ✗ Download failed: ${res.status}`);
    process.exit(1);
  }
  const text = await res.text();
  writeFileSync(OFSTED_RAW, text);
  console.log(`  ✓ Downloaded ${(text.length / 1024 / 1024).toFixed(1)}MB`);
} else {
  console.log(`  ✓ Using cached Ofsted data`);
}

// Parse CSV properly (handles quoted commas)
function parseCSV(content) {
  const lines = content.split('\n');
  const rows = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    const cols = [];
    let cur = '';
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
        else inQ = !inQ;
      } else if (ch === ',' && !inQ) {
        cols.push(cur.trim());
        cur = '';
      } else {
        cur += ch;
      }
    }
    cols.push(cur.trim());
    rows.push(cols);
  }
  return rows;
}

const content = readFileSync(OFSTED_RAW, 'utf-8');
const rows = parseCSV(content);
const headers = rows[0].map(h => h.toLowerCase());

const colUrn = headers.findIndex(h => h === 'urn');
const colPostcode = headers.findIndex(h => h === 'postcode');
const colOverall = headers.findIndex(h => h.includes('overall effectiveness'));
const colName = headers.findIndex(h => h === 'school name');

console.log(`  Columns: urn=${colUrn}, postcode=${colPostcode}, overall=${colOverall}, name=${colName}`);
console.log(`  Total rows: ${rows.length - 1}`);

// Grade map: Ofsted 1-4 → our labels
const GRADE_MAP = {
  '1': 'Outstanding',
  '2': 'Good',
  '3': 'Requires improvement',
  '4': 'Inadequate',
};

// Build URN → grade lookup
const urnToGrade = {};
let graded = 0;
for (const row of rows.slice(1)) {
  const urn = row[colUrn]?.trim();
  const grade = GRADE_MAP[row[colOverall]?.trim()];
  if (urn && grade) {
    urnToGrade[urn] = grade;
    graded++;
  }
}
console.log(`  ${graded} schools with Ofsted grades`);

// Now load GIAS schools data and merge Ofsted grades
const giasPath = join(RAW_DIR, 'schools-gias.csv');
if (!existsSync(giasPath)) {
  console.error('  ✗ schools-gias.csv not found. Run 01-download.mjs first.');
  process.exit(1);
}

const giasContent = readFileSync(giasPath, 'utf-8');
const giasRows = parseCSV(giasContent);
const giasHeaders = giasRows[0].map(h => h.toLowerCase().replace(/[^a-z0-9]/g, '_'));

function giasCol(name) { return giasHeaders.findIndex(h => h.includes(name)); }
const gColUrn = giasCol('urn');
const gColName = giasHeaders.findIndex(h => h === 'establishmentname');
const gColPostcode = giasCol('postcode');
const gColStatus = giasHeaders.findIndex(h => h.includes('establishmentstatus') && h.includes('name'));
const gColPhase = giasHeaders.findIndex(h => h.includes('phaseofed') && h.includes('name'));
const gColType = giasHeaders.findIndex(h => h.includes('typeofestablishment') && h.includes('name'));

console.log(`  GIAS rows: ${giasRows.length - 1}`);

function toOutcode(postcode) {
  if (!postcode) return null;
  return postcode.trim().split(' ')[0]?.toUpperCase() || null;
}

const STATUS_OPEN = new Set(['Open', 'Open, but proposed to close']);

// Build schools-by-outcode with real Ofsted grades
const byOutcode = {};
let matched = 0;

for (const row of giasRows.slice(1)) {
  const status = row[gColStatus]?.trim();
  if (!STATUS_OPEN.has(status)) continue;

  const postcode = row[gColPostcode]?.trim();
  const outcode = toOutcode(postcode);
  if (!outcode) continue;

  const urn = row[gColUrn]?.trim();
  const name = row[gColName]?.trim() || 'Unknown School';
  const phase = row[gColPhase]?.trim() || '';
  const type = row[gColType]?.trim() || '';

  const ofsted = urnToGrade[urn] || null;
  if (ofsted) matched++;

  const schoolType = phase.includes('Primary') ? 'Primary'
    : phase.includes('Secondary') ? 'Secondary'
    : phase.includes('16') ? 'Post-16'
    : type.includes('nursery') || type.includes('Nursery') ? 'Nursery'
    : 'Other';

  if (!byOutcode[outcode]) {
    byOutcode[outcode] = {
      count: 0,
      outstanding: 0,
      good: 0,
      requiresImprovement: 0,
      inadequate: 0,
      notInspected: 0,
      nearest: [],
    };
  }

  const oc = byOutcode[outcode];
  oc.count++;

  if (ofsted === 'Outstanding') oc.outstanding++;
  else if (ofsted === 'Good') oc.good++;
  else if (ofsted === 'Requires improvement') oc.requiresImprovement++;
  else if (ofsted === 'Inadequate') oc.inadequate++;
  else oc.notInspected++;

  if (oc.nearest.length < 5) {
    oc.nearest.push({
      name,
      type: schoolType,
      ofsted: ofsted || 'Not inspected',
    });
  }
}

const outcodeCount = Object.keys(byOutcode).length;
console.log(`  ${outcodeCount} outcodes with school data`);
console.log(`  ${matched} school-outcode pairs with Ofsted grades`);

writeFileSync(join(PROCESSED_DIR, 'schools-by-outcode.json'), JSON.stringify(byOutcode, null, 2));
console.log(`\n  ✓ Saved schools-by-outcode.json (with real Ofsted grades)`);
console.log('  Next: node pipeline/09-aggregate.mjs\n');
