/**
 * Step 4: Parse DfE Get Information About Schools (GIAS) data.
 * Run: node pipeline/04-parse-schools.mjs
 *
 * Input:  pipeline/raw/schools-gias.csv
 * Output: pipeline/processed/schools-by-outcode.json
 *
 * Key columns in GIAS export:
 * URN, EstablishmentName, TypeOfEstablishment, EstablishmentStatus,
 * ReasonEstablishmentOpened, OpenDate, CloseDate, PhaseOfEducation,
 * StatutoryLowAge, StatutoryHighAge, Boarders, OfficialSixthForm,
 * Postcode, Ofsted rating (OfstedRating or InspectionResult)
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const PROCESSED_DIR = join(__dirname, 'processed');
mkdirSync(PROCESSED_DIR, { recursive: true });

function toOutcode(postcode) {
  if (!postcode) return null;
  const parts = postcode.trim().split(' ');
  return parts[0]?.toUpperCase() || null;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') {
      inQuotes = !inQuotes;
    } else if (line[i] === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += line[i];
    }
  }
  result.push(current.trim());
  return result;
}

const OFSTED_MAP = {
  '1': 'Outstanding',
  '2': 'Good',
  '3': 'Requires improvement',
  '4': 'Inadequate',
  'Outstanding': 'Outstanding',
  'Good': 'Good',
  'Requires improvement': 'Requires improvement',
  'Inadequate': 'Inadequate',
  'Special Measures': 'Inadequate',
};

const STATUS_OPEN = new Set(['Open', 'Open, but proposed to close']);

console.log('\n=== Step 4: Parse DfE Schools Data ===\n');

const schoolsPath = join(RAW_DIR, 'schools-gias.csv');
if (!existsSync(schoolsPath)) {
  console.warn('  ⚠ schools-gias.csv not found. Generating empty schools data.');
  writeFileSync(join(PROCESSED_DIR, 'schools-by-outcode.json'), '{}');
  process.exit(0);
}

const content = readFileSync(schoolsPath, 'utf-8');
const lines = content.split('\n');
const headers = parseCSVLine(lines[0]).map(h => h.toLowerCase().replace(/[^a-z0-9]/g, '_'));

console.log(`  Headers found: ${headers.length}`);

// Find key column indices — prefer _name_ variant over _code_ for columns with both
const col = (name) => headers.findIndex(h => h.includes(name));
const colName = (name) => {
  // Prefer the (name) variant for columns that have both code and name variants
  const nameIdx = headers.findIndex(h => h.includes(name) && h.includes('name'));
  if (nameIdx !== -1) return nameIdx;
  return headers.findIndex(h => h.includes(name));
};
const colPostcode = col('postcode');
const colEstName = col('establishmentname') !== -1 ? col('establishmentname') : col('_name_') !== -1 ? headers.findIndex(h => h === '_name_') : 4;
const colStatus = colName('establishmentstatus'); // prefer (name) variant → "Open"
const colPhase = colName('phaseofed') !== -1 ? colName('phaseofed') : colName('phase');
// GIAS doesn't include Ofsted ratings — no colOfsted available in this dataset
const colOfsted = -1;
const colType = colName('typeofestablishment') !== -1 ? colName('typeofestablishment') : colName('type');
const colLowAge = col('statutorylowage');
const colHighAge = col('statutoryhighage');
const colLat = col('latitude');
const colLng = col('longitude');

console.log(`  Key columns: postcode=${colPostcode}, ofsted=${colOfsted}, phase=${colPhase}`);

const byOutcode = new Map();
let count = 0;
let skipped = 0;

for (let i = 1; i < lines.length; i++) {
  const line = lines[i];
  if (!line.trim()) continue;

  const cols = parseCSVLine(line);
  if (cols.length < 5) { skipped++; continue; }

  const status = colStatus !== -1 ? cols[colStatus] : 'Open';
  if (!STATUS_OPEN.has(status)) { skipped++; continue; }

  const postcode = colPostcode !== -1 ? cols[colPostcode] : null;
  if (!postcode) { skipped++; continue; }

  const outcode = toOutcode(postcode);
  if (!outcode) { skipped++; continue; }

  const name = colEstName !== -1 ? cols[colEstName] : 'School';
  const ofstedRaw = colOfsted !== -1 ? cols[colOfsted] : '';
  const ofsted = OFSTED_MAP[ofstedRaw] || null;

  const phase = colPhase !== -1 ? cols[colPhase] : '';
  const lowAge = colLowAge !== -1 ? parseInt(cols[colLowAge], 10) : null;
  const highAge = colHighAge !== -1 ? parseInt(cols[colHighAge], 10) : null;

  let type = 'School';
  if (phase?.includes('Primary') || (lowAge !== null && lowAge <= 7)) type = 'Primary';
  else if (phase?.includes('Secondary') || (lowAge !== null && lowAge >= 11)) type = 'Secondary';
  else if (phase?.includes('16') || (lowAge !== null && lowAge >= 16)) type = 'Sixth Form';
  else if (phase?.includes('All')) type = 'All-through';

  const lat = colLat !== -1 ? parseFloat(cols[colLat]) : null;
  const lng = colLng !== -1 ? parseFloat(cols[colLng]) : null;

  if (!byOutcode.has(outcode)) {
    byOutcode.set(outcode, {
      schools: [],
      outstanding: 0,
      good: 0,
      requiresImprovement: 0,
      inadequate: 0,
    });
  }

  const entry = byOutcode.get(outcode);
  if (ofsted === 'Outstanding') entry.outstanding++;
  else if (ofsted === 'Good') entry.good++;
  else if (ofsted === 'Requires improvement') entry.requiresImprovement++;
  else if (ofsted === 'Inadequate') entry.inadequate++;

  entry.schools.push({ name, type, ofsted, lat, lng });
  count++;
}

console.log(`  Parsed ${count.toLocaleString()} open schools across ${byOutcode.size} outcodes`);

// Build output: top 5 nearest schools per outcode (sorted by Ofsted rating)
const output = {};
const ofstedOrder = { 'Outstanding': 0, 'Good': 1, 'Requires improvement': 2, 'Inadequate': 3, null: 4 };

for (const [outcode, data] of byOutcode) {
  const sorted = [...data.schools].sort((a, b) => {
    return (ofstedOrder[a.ofsted] ?? 4) - (ofstedOrder[b.ofsted] ?? 4);
  });

  output[outcode] = {
    count: data.schools.length,
    outstanding: data.outstanding,
    good: data.good,
    requiresImprovement: data.requiresImprovement,
    inadequate: data.inadequate,
    nearest: sorted.slice(0, 5).map(s => ({
      name: s.name,
      type: s.type,
      ofsted: s.ofsted || 'Not yet inspected',
      distance: 0, // Will be estimated from coordinates in aggregate step
    })),
  };
}

const outputPath = join(PROCESSED_DIR, 'schools-by-outcode.json');
writeFileSync(outputPath, JSON.stringify(output, null, 2));
console.log(`\n✅ Schools processing complete`);
console.log(`   ${Object.keys(output).length} outcodes with school data`);
console.log(`   Output: pipeline/processed/schools-by-outcode.json\n`);
