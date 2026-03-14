/**
 * pipeline/12b-fetch-imd.mjs
 * Download IMD 2019 LA-level summaries (File 10 = lower-tier LAs)
 * Source: MHCLG via assets.publishing.service.gov.uk
 */
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW = join(__dirname, 'raw');
const OUT = join(__dirname, 'processed', 'imd-by-outcode.json');
const OUTCODE_LA_CODE_CACHE = join(__dirname, 'raw', 'outcode-la-code-cache.json');
const OUTCODE_LA_CACHE = join(__dirname, 'raw', 'outcode-la-cache.json');

// File 10 = lower-tier LA summaries (district/unitary)
const IMD_URL = 'https://assets.publishing.service.gov.uk/media/5d8b3cfbe5274a08be69aa91/File_10_-_IoD2019_Local_Authority_District_Summaries__lower-tier__.xlsx';
const xlsxPath = join(RAW, 'imd-file10.xlsx');
const extractDir = join(RAW, 'imd-file10-extract');

// Download
if (!existsSync(xlsxPath)) {
  console.log('Downloading IMD File 10...');
  const res = await fetch(IMD_URL);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  writeFileSync(xlsxPath, Buffer.from(buf));
  console.log(`Downloaded ${Math.round(buf.byteLength / 1024)}KB`);
} else {
  console.log('Using cached IMD file');
}

// Extract XML
if (!existsSync(extractDir)) mkdirSync(extractDir, { recursive: true });
execSync(`cd "${extractDir}" && unzip -o "${xlsxPath}" xl/worksheets/sheet1.xml xl/sharedStrings.xml 2>/dev/null || true`);

// Parse shared strings
function parseSharedStrings(xml) {
  const strings = [];
  const regex = /<si>[\s\S]*?<\/si>/g;
  const tRegex = /<t[^>]*>([^<]*)<\/t>/;
  let m;
  while ((m = regex.exec(xml)) !== null) {
    const t = tRegex.exec(m[0]);
    strings.push(t ? t[1].replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').trim() : '');
  }
  return strings;
}

// Parse sheet XML into rows
function parseSheet(xml, sharedStrings) {
  const rows = [];
  const rowRe = /<row[^>]*>([\s\S]*?)<\/row>/g;
  const cellRe = /<c r="([A-Z]+)(\d+)"([^>]*)>([\s\S]*?)<\/c>/g;
  const valRe = /<v>([^<]*)<\/v>/;

  let rowMatch;
  while ((rowMatch = rowRe.exec(xml)) !== null) {
    const row = {};
    let cellMatch;
    while ((cellMatch = cellRe.exec(rowMatch[1])) !== null) {
      const col = cellMatch[1];
      const attrs = cellMatch[3];
      const content = cellMatch[4];
      const valMatch = valRe.exec(content);
      if (!valMatch) continue;
      let val = valMatch[1];
      if (attrs.includes('t="s"')) val = sharedStrings[parseInt(val)] ?? val;
      row[col] = val;
    }
    if (Object.keys(row).length > 0) rows.push(row);
  }
  return rows;
}

const ssPath = join(extractDir, 'xl', 'sharedStrings.xml');
const sheetPath = join(extractDir, 'xl', 'worksheets', 'sheet1.xml');

const sharedStrings = existsSync(ssPath) ? parseSharedStrings(readFileSync(ssPath, 'utf-8')) : [];
const rows = parseSheet(readFileSync(sheetPath, 'utf-8'), sharedStrings);

console.log(`Parsed ${rows.length} rows`);
if (rows.length > 0) {
  console.log('Row 1 (header):', JSON.stringify(rows[0]));
  console.log('Row 2 (data):', JSON.stringify(rows[1]));
}

// File 10 structure:
// Row 1: headers
// Col A = LA code, Col B = LA name
// IMD average score = find by header name
// Find header row
let headerRow = rows[0];
console.log('\nDetecting columns...');

// Map column letter → field name by header content
const colMap = {};
for (const [col, val] of Object.entries(headerRow)) {
  const v = String(val).toLowerCase();
  if (v.includes('district code') || v === 'la code' || (v.includes('code') && col === 'A')) colMap.code = col;
  if (v.includes('district name') || v === 'la name' || (v.includes('name') && col === 'B')) colMap.name = col;
  if (v.includes('average score') && v.includes('imd')) colMap.score = col;
  if (v.includes('average rank') && v.includes('imd')) colMap.rank = col;
  if (v.includes('proportion') && v.includes('most deprived 10%')) colMap.pct10 = col;
}

// Fallback: if headers not detected, try to infer from structure
if (!colMap.code) {
  // Try first 3 rows to find the header row
  for (let i = 0; i < Math.min(5, rows.length); i++) {
    const r = rows[i];
    for (const [col, val] of Object.entries(r)) {
      const v = String(val).toLowerCase();
      if (v.includes('code')) { colMap.code = col; break; }
    }
    if (colMap.code) { headerRow = r; break; }
  }
}

console.log('Column map:', colMap);

// Build LA code → IMD data
const imdByLA = {};
let dataStart = rows.indexOf(headerRow) + 1;
if (dataStart <= 0) dataStart = 1;

for (let i = dataStart; i < rows.length; i++) {
  const row = rows[i];
  const code = colMap.code ? String(row[colMap.code] || '').trim() : Object.values(row)[0];
  if (!code || !/^E\d{8}$/.test(code)) continue;

  const score = colMap.score ? parseFloat(String(row[colMap.score] || '')) : null;
  const rank = colMap.rank ? parseInt(String(row[colMap.rank] || '')) : null;
  const pct10 = colMap.pct10 ? parseFloat(String(row[colMap.pct10] || '')) : null;

  imdByLA[code] = {
    score: score && !isNaN(score) ? Math.round(score * 10) / 10 : null,
    rank: rank && !isNaN(rank) ? rank : null,
    pct10: pct10 && !isNaN(pct10) ? Math.round(pct10 * 1000) / 10 : null,
  };
}

console.log(`Built IMD data for ${Object.keys(imdByLA).length} LAs`);
if (Object.keys(imdByLA).length > 0) {
  console.log('Sample:', Object.entries(imdByLA).slice(0, 2));
}

// Map outcode → IMD
let laCodeMap = {};
if (existsSync(OUTCODE_LA_CODE_CACHE)) {
  laCodeMap = JSON.parse(readFileSync(OUTCODE_LA_CODE_CACHE, 'utf-8'));
  console.log(`Loaded ${Object.keys(laCodeMap).length} outcode→LA codes`);
}

if (Object.keys(laCodeMap).length < 100) {
  // Build from postcodes.io
  const outcodeLA = JSON.parse(readFileSync(OUTCODE_LA_CACHE, 'utf-8'));
  const outcodes = Object.keys(outcodeLA);
  console.log(`Building outcode→LA code mapping for ${outcodes.length} outcodes...`);
  const CONCURRENCY = 20;
  for (let i = 0; i < outcodes.length; i += CONCURRENCY) {
    const batch = outcodes.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (outcode) => {
      try {
        const res = await fetch(`https://api.postcodes.io/outcodes/${encodeURIComponent(outcode)}`);
        if (!res.ok) return;
        const json = await res.json();
        const codes = json?.result?.admin_district_code;
        if (codes?.length) laCodeMap[outcode.toUpperCase()] = codes[0];
      } catch {}
    }));
    if ((i + CONCURRENCY) % 400 === 0) process.stdout.write(`${i}/${outcodes.length} `);
    await new Promise(r => setTimeout(r, 50));
  }
  console.log(`\nBuilt ${Object.keys(laCodeMap).length} entries`);
  writeFileSync(OUTCODE_LA_CODE_CACHE, JSON.stringify(laCodeMap));
}

const result = {};
let matched = 0;
for (const [outcode, laCode] of Object.entries(laCodeMap)) {
  if (imdByLA[laCode]) {
    result[outcode] = imdByLA[laCode];
    matched++;
  }
}

console.log(`Matched ${matched} outcodes to IMD data`);
writeFileSync(OUT, JSON.stringify(result));
console.log(`Written to ${OUT}`);
