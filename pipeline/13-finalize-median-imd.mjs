/**
 * pipeline/13-finalize-median-imd.mjs
 * Fix: compute real median age + IMD per outcode
 *
 * Median age: NOMIS NM_31_1 age bands → compute median → map via LA name
 * IMD: MHCLG File 10 sheet2.xml → parse → map via LA name
 */
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW = join(__dirname, 'raw');
const PROCESSED = join(__dirname, 'processed');
const OUTCODE_LA_CACHE = join(RAW, 'outcode-la-cache.json');

// ============================================================
// PART 1: MEDIAN AGE
// ============================================================
console.log('\n=== MEDIAN AGE ===');

// Fetch NOMIS with GEOGRAPHY_NAME included (no select filter)
const AGE_CODES = '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20';
const AGE_BAND_MID = [0.5, 2.5, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57, 62, 67, 72, 77, 82, 87, 90];

const ageCacheWithNames = join(RAW, 'la-age-bands-named.csv');

async function fetchAgeWithNames() {
  if (existsSync(ageCacheWithNames)) {
    console.log('Using cached age data with names');
    return readFileSync(ageCacheWithNames, 'utf-8');
  }
  const url = `https://www.nomisweb.co.uk/api/v01/dataset/NM_31_1/data.csv?geography=TYPE464&sex=7&measures=20100&age=${AGE_CODES}&date=latestMINUS1`;
  console.log('Fetching NOMIS age data with geography names...');
  const res = await fetch(url);
  if (!res.ok) throw new Error(`NOMIS HTTP ${res.status}`);
  const csv = await res.text();
  writeFileSync(ageCacheWithNames, csv);
  return csv;
}

function computeMedianByLA(csv) {
  const lines = csv.trim().split('\n');
  const header = lines[0].split(',').map(h => h.replace(/"/g, '').toLowerCase().trim());
  const geoCodeIdx = header.indexOf('geography_code');
  const geoNameIdx = header.indexOf('geography_name');
  const ageCodeIdx = header.indexOf('age_code');
  const valIdx = header.indexOf('obs_value');

  console.log(`Columns: geoCode=${geoCodeIdx}, geoName=${geoNameIdx}, ageCode=${ageCodeIdx}, val=${valIdx}`);

  const byLA = new Map(); // code → { name, bands[] }
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',').map(v => v.replace(/"/g, '').trim());
    const geo = cols[geoCodeIdx];
    const name = cols[geoNameIdx];
    const ageCode = parseInt(cols[ageCodeIdx]);
    const val = parseInt(cols[valIdx]);
    if (!geo || isNaN(ageCode) || isNaN(val) || ageCode < 1 || ageCode > 20) continue;

    if (!byLA.has(geo)) byLA.set(geo, { name: name || '', bands: new Array(20).fill(0) });
    byLA.get(geo).bands[ageCode - 1] = val;
  }

  const result = {}; // code → { name, medianAge }
  for (const [code, { name, bands }] of byLA) {
    const total = bands.reduce((s, v) => s + v, 0);
    if (!total) continue;
    let cumulative = 0;
    const half = total / 2;
    for (let i = 0; i < bands.length; i++) {
      cumulative += bands[i];
      if (cumulative >= half) {
        result[code] = { name: name.toLowerCase(), median: Math.round(AGE_BAND_MID[i]) };
        break;
      }
    }
  }
  return result;
}

// Load outcode → LA name map
const outcodeLA = JSON.parse(readFileSync(OUTCODE_LA_CACHE, 'utf-8'));

// Build normalised LA name → code from NOMIS
const ageCsv = await fetchAgeWithNames();
const laMedianMap = computeMedianByLA(ageCsv);
console.log(`Computed median age for ${Object.keys(laMedianMap).length} LAs`);

// Build name → code lookup (normalised)
const nameToCode = new Map();
for (const [code, { name }] of Object.entries(laMedianMap)) {
  nameToCode.set(name.toLowerCase(), code);
}

// Map outcode → median age
const medianAgeByOutcode = {};
let matched = 0;
for (const [outcode, laName] of Object.entries(outcodeLA)) {
  if (!laName) continue;
  const norm = laName.toLowerCase();
  const code = nameToCode.get(norm);
  if (code && laMedianMap[code]) {
    medianAgeByOutcode[outcode.toUpperCase()] = laMedianMap[code].median;
    matched++;
  }
}
console.log(`Matched ${matched}/${Object.keys(outcodeLA).length} outcodes to median age`);

// Try fuzzy match for unmatched (common name variations)
const unmatched = Object.keys(outcodeLA).filter(o => !medianAgeByOutcode[o.toUpperCase()]);
console.log(`Unmatched: ${unmatched.length} — trying fuzzy match...`);
for (const outcode of unmatched) {
  if (!outcodeLA[outcode]) continue;
  const laName = outcodeLA[outcode].toLowerCase();
  // Try partial match
  for (const [normName, code] of nameToCode) {
    if (normName.includes(laName) || laName.includes(normName)) {
      medianAgeByOutcode[outcode.toUpperCase()] = laMedianMap[code].median;
      break;
    }
  }
}
matched = Object.keys(medianAgeByOutcode).length;
console.log(`After fuzzy: ${matched} outcodes matched`);

writeFileSync(join(PROCESSED, 'median-age-by-outcode.json'), JSON.stringify(medianAgeByOutcode));
console.log('Written median-age-by-outcode.json');

// ============================================================
// PART 2: IMD
// ============================================================
console.log('\n=== IMD ===');

const xlsxPath = join(RAW, 'imd-file10.xlsx');
const extractDir = join(RAW, 'imd-file10-extract');

if (!existsSync(xlsxPath)) {
  console.log('Downloading IMD File 10...');
  const res = await fetch('https://assets.publishing.service.gov.uk/media/5d8b3cfbe5274a08be69aa91/File_10_-_IoD2019_Local_Authority_District_Summaries__lower-tier__.xlsx');
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  writeFileSync(xlsxPath, Buffer.from(buf));
}

// Extract sheet2.xml (data is on sheet2, not sheet1)
mkdirSync(join(extractDir, 'xl', 'worksheets'), { recursive: true });
execSync(`cd "${extractDir}" && unzip -o "${xlsxPath}" "xl/worksheets/sheet2.xml" "xl/sharedStrings.xml" 2>/dev/null || true`);

const ssPath = join(extractDir, 'xl', 'sharedStrings.xml');
const sheetPath = join(extractDir, 'xl', 'worksheets', 'sheet2.xml');

if (!existsSync(sheetPath)) {
  console.error('sheet2.xml not found after extraction');
  process.exit(1);
}

// Parse shared strings
function parseSharedStrings(xml) {
  const strings = [];
  // Handle both <t>text</t> and <t xml:space="preserve">text</t>
  const siRe = /<si>([\s\S]*?)<\/si>/g;
  const tRe = /<t[^>]*>([^<]*)<\/t>/g;
  let m;
  while ((m = siRe.exec(xml)) !== null) {
    const texts = [];
    let t;
    while ((t = tRe.exec(m[1])) !== null) texts.push(t[1]);
    strings.push(texts.join('').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').trim());
  }
  return strings;
}

// Parse rows from sheet XML
function parseRows(xml, ss) {
  const rows = [];
  const rowRe = /<row\b[^>]*>([\s\S]*?)<\/row>/g;
  const cellRe = /<c\b r="([A-Z]+)\d+"([^>]*)>([\s\S]*?)<\/c>/g;
  const valRe = /<v>([^<]*)<\/v>/;

  let rm;
  while ((rm = rowRe.exec(xml)) !== null) {
    const row = {};
    let cm;
    while ((cm = cellRe.exec(rm[1])) !== null) {
      const col = cm[1];
      const attrs = cm[2];
      const vm = valRe.exec(cm[3]);
      if (!vm) continue;
      let v = vm[1];
      if (/t="s"/.test(attrs)) v = ss[parseInt(v)] ?? v;
      row[col] = v;
    }
    if (Object.keys(row).length > 0) rows.push(row);
  }
  return rows;
}

const ss = existsSync(ssPath) ? parseSharedStrings(readFileSync(ssPath, 'utf-8')) : [];
const sheetXml = readFileSync(sheetPath, 'utf-8');
const rows = parseRows(sheetXml, ss);
console.log(`Parsed ${rows.length} rows from sheet2`);

if (rows.length > 0) {
  console.log('Row 0:', JSON.stringify(rows[0]));
  console.log('Row 1:', JSON.stringify(rows[1]));
  console.log('Row 2:', JSON.stringify(rows[2]));
}

// File 10 sheet 2 = IMD domain
// Find data rows: LA code pattern E[0-9]{8}
// The sheet might have a title row(s) before headers
// Look for the row containing the LA code header
let headerIdx = -1;
for (let i = 0; i < Math.min(10, rows.length); i++) {
  const vals = Object.values(rows[i]).map(v => String(v));
  // Header row contains 'Local Authority District code'
  if (vals.some(v => /code/i.test(v) || /E\d{8}/.test(v))) {
    // Check if it's a header (no E-code pattern) or first data row
    if (!vals.some(v => /^E\d{8}$/.test(v))) {
      headerIdx = i;
    } else {
      headerIdx = i - 1; // data starts here, header was previous
    }
    break;
  }
}

console.log(`Header row index: ${headerIdx}`);
if (headerIdx >= 0 && headerIdx < rows.length) {
  console.log('Header row:', JSON.stringify(rows[headerIdx]));
}

// Find column positions for code, name, IMD score, IMD rank, % most deprived 10%
// IMD File 10 structure (approximate):
// A: LA code, B: LA name
// Then columns for each domain...
// IMD summary columns: score, rank, % in most deprived 10%, % in most deprived 20%

// Try to find by scanning header row
const headerRow = headerIdx >= 0 ? rows[headerIdx] : rows[0];
let codeCol = 'A', nameCol = 'B', scoreCol = null, rankCol = null, pctCol = null;

for (const [col, val] of Object.entries(headerRow || {})) {
  const v = String(val).toLowerCase();
  // Exclude "rank of X" — want the actual value columns
  if (v.includes('average score') && !v.includes('rank of')) scoreCol = col;
  if (v.includes('average rank') && !v.includes('rank of')) rankCol = col;
  if (v.includes('proportion') && v.includes('most deprived 10%') && !v.includes('rank of')) pctCol = col;
}

console.log(`Columns — code:${codeCol} name:${nameCol} score:${scoreCol} rank:${rankCol} pct:${pctCol}`);

// Parse data rows
const imdByName = {};
const dataStart = Math.max(0, headerIdx + 1);
for (let i = dataStart; i < rows.length; i++) {
  const row = rows[i];
  const code = String(row[codeCol] || '').trim();
  const name = String(row[nameCol] || '').trim().toLowerCase();
  if (!code || !/^E\d{8}$/.test(code)) continue;

  const score = scoreCol ? parseFloat(row[scoreCol]) : null;
  const rank = rankCol ? parseInt(row[rankCol]) : null;
  const pct = pctCol ? parseFloat(row[pctCol]) : null;

  // IMD rank: 1 = most deprived, 317 = least deprived
  // Decile 1 = most deprived, 10 = least deprived
  const totalLAs = 317;
  const decile = rank ? Math.ceil((rank / totalLAs) * 10) : null;

  imdByName[name] = {
    score: score && !isNaN(score) ? Math.round(score * 10) / 10 : null,
    rank: rank && !isNaN(rank) ? rank : null,
    decile: decile && decile >= 1 && decile <= 10 ? decile : null,
    pct10: pct && !isNaN(pct) ? Math.round(pct * 10) / 10 : null,
  };
}

console.log(`Built IMD data for ${Object.keys(imdByName).length} LAs`);
if (Object.keys(imdByName).length > 0) {
  console.log('Sample:', Object.entries(imdByName).slice(0, 3));
}

// Map outcode → IMD via LA name
const imdByOutcode = {};
let imdMatched = 0;
for (const [outcode, laName] of Object.entries(outcodeLA)) {
  if (!laName) continue;
  const norm = laName.toLowerCase();
  if (imdByName[norm]) {
    imdByOutcode[outcode.toUpperCase()] = imdByName[norm];
    imdMatched++;
  }
}
console.log(`Direct match: ${imdMatched} outcodes`);

// Fuzzy match remaining
for (const [outcode, laName] of Object.entries(outcodeLA)) {
  if (imdByOutcode[outcode.toUpperCase()]) continue;
  if (!laName) continue;
  const norm = laName.toLowerCase();
  for (const [imdName, data] of Object.entries(imdByName)) {
    if (imdName.includes(norm) || norm.includes(imdName)) {
      imdByOutcode[outcode.toUpperCase()] = data;
      break;
    }
  }
}
console.log(`After fuzzy: ${Object.keys(imdByOutcode).length} outcodes`);

writeFileSync(join(PROCESSED, 'imd-by-outcode.json'), JSON.stringify(imdByOutcode));
console.log('Written imd-by-outcode.json');
