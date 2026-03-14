/**
 * pipeline/12-fetch-imd.mjs
 * Download English Indices of Multiple Deprivation (IMD) 2019 LA summary
 * Source: MHCLG File_11 — IoD2019 Local Authority District Summaries (lower-tier)
 * Outputs: pipeline/processed/imd-by-outcode.json
 * Fields: imdScore (avg), imdRank (1=most deprived), imdDecile1Pct (% of LSOAs in most deprived 10%)
 */
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW = join(__dirname, 'raw');
const OUT = join(__dirname, 'processed', 'imd-by-outcode.json');

// IMD 2019 LA summaries CSV (directly downloadable)
// MHCLG also publishes this as a CSV via open data
const IMD_CSV_URL = 'https://assets.publishing.service.gov.uk/media/5d8b3b51ed915d53670ac8f8/File_11_-_IoD2019_Local_Authority_District_Summaries__lower-tier__.xlsx';

// Alternative: use the CSV version of the LSOA data from ONS open data platform
// But LA summaries are easiest. Let's use the direct CSV if available.
// MHCLG also provides: https://opendatacommunities.org/resource?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fdeprivation%2Findices-deprivation-2019-lad

// Simpler: parse the CSV version available from the Exeter data portal
const IMD_LA_CSV = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833973/File_11_-_IoD2019_Local_Authority_District_Summaries__lower-tier__.xlsx';

// Actually let's use the open data CSV version
// ONS / MHCLG publish this as CSV too — try the government data API
const LA_SUMMARIES_URL = 'https://opendatacommunities.org/downloads/node/29050';

// Use the XLSX approach with unzip (it's an Office Open XML file = ZIP)
async function downloadFile(url, destPath) {
  if (existsSync(destPath)) {
    console.log(`Using cached ${destPath}`);
    return;
  }
  console.log(`Downloading ${url}...`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const buf = await res.arrayBuffer();
  writeFileSync(destPath, Buffer.from(buf));
  console.log(`Saved ${destPath}`);
}

// Parse XLSX (Office Open XML) — extract the data from xl/worksheets/sheet1.xml
import { execSync } from 'node:child_process';

async function parseXlsx(xlsxPath, extractDir) {
  if (!existsSync(extractDir)) mkdirSync(extractDir, { recursive: true });
  execSync(`cd "${extractDir}" && unzip -o "${xlsxPath}" xl/worksheets/sheet1.xml xl/sharedStrings.xml 2>/dev/null || true`);
}

function parseSharedStrings(xmlContent) {
  const strings = [];
  const siRegex = /<si>[\s\S]*?<\/si>/g;
  const tRegex = /<t[^>]*>([\s\S]*?)<\/t>/;
  let match;
  while ((match = siRegex.exec(xmlContent)) !== null) {
    const tMatch = tRegex.exec(match[0]);
    strings.push(tMatch ? tMatch[1].replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>') : '');
  }
  return strings;
}

function parseSheet(xmlContent, sharedStrings) {
  const rows = [];
  const rowRegex = /<row[^>]*>([\s\S]*?)<\/row>/g;
  const cellRegex = /<c r="([A-Z]+\d+)"([^>]*)>([\s\S]*?)<\/c>/g;
  const valRegex = /<v>([\s\S]*?)<\/v>/;

  let rowMatch;
  while ((rowMatch = rowRegex.exec(xmlContent)) !== null) {
    const rowData = {};
    let cellMatch;
    while ((cellMatch = cellRegex.exec(rowMatch[1])) !== null) {
      const cellRef = cellMatch[1];
      const cellType = cellMatch[2];
      const cellContent = cellMatch[3];
      const valMatch = valRegex.exec(cellContent);
      if (!valMatch) continue;

      const col = cellRef.replace(/\d+/, '');
      let val = valMatch[1];
      if (cellType.includes('t="s"')) {
        val = sharedStrings[parseInt(val)] || val;
      }
      rowData[col] = val;
    }
    if (Object.keys(rowData).length > 0) rows.push(rowData);
  }
  return rows;
}

const xlsxPath = join(RAW, 'imd-la-summaries.xlsx');

try {
  await downloadFile(IMD_LA_CSV, xlsxPath);
} catch (e) {
  // Try alternative URL
  try {
    await downloadFile('https://assets.publishing.service.gov.uk/media/5d8b3b51ed915d53670ac8f8/File_11_-_IoD2019_Local_Authority_District_Summaries__lower-tier__.xlsx', xlsxPath);
  } catch (e2) {
    console.error('Both download URLs failed:', e2.message);
    process.exit(1);
  }
}

const extractDir = join(RAW, 'imd-extract');
await parseXlsx(xlsxPath, extractDir);

const sharedStringsPath = join(extractDir, 'xl', 'sharedStrings.xml');
const sheetPath = join(extractDir, 'xl', 'worksheets', 'sheet1.xml');

if (!existsSync(sheetPath)) {
  console.error('Could not extract sheet XML');
  process.exit(1);
}

const sharedStrings = existsSync(sharedStringsPath)
  ? parseSharedStrings(readFileSync(sharedStringsPath, 'utf-8'))
  : [];

const rows = parseSheet(readFileSync(sheetPath, 'utf-8'), sharedStrings);

console.log(`Parsed ${rows.length} rows from IMD LA summaries`);
if (rows.length > 0) {
  console.log('Sample row:', JSON.stringify(rows[1]));
  console.log('Header row:', JSON.stringify(rows[0]));
}

// The LA summaries file format:
// Col A: LA code (E07000098)
// Col B: LA name
// Col C: IMD - Average score
// Col D: IMD - Average rank
// Col E: IMD - Proportion of LSOAs in most deprived 10% nationally
// (exact columns may vary — detect from header row)

// Find header row
let headerRowIdx = 0;
for (let i = 0; i < Math.min(5, rows.length); i++) {
  const vals = Object.values(rows[i]).map(v => String(v).toLowerCase());
  if (vals.some(v => v.includes('local authority') || v.includes('la code') || v.includes('code'))) {
    headerRowIdx = i;
    break;
  }
}
const headerRow = rows[headerRowIdx];
console.log('Header row detected:', JSON.stringify(headerRow));

// Build column map
const colMap = {};
for (const [col, val] of Object.entries(headerRow)) {
  const v = String(val).toLowerCase();
  if (v.includes('code') || v.includes('la code')) colMap.code = col;
  if (v.includes('average score') || (v.includes('score') && !v.includes('rank'))) colMap.score = col;
  if (v.includes('proportion') || v.includes('most deprived') || v.includes('%')) colMap.pct = col;
  if (v.includes('average rank') || v.includes('rank')) colMap.rank = col;
}
console.log('Column map:', colMap);

// Build LA code → IMD data
const imdByLA = {};
for (let i = headerRowIdx + 1; i < rows.length; i++) {
  const row = rows[i];
  const code = colMap.code ? row[colMap.code]?.trim() : null;
  if (!code || !code.startsWith('E')) continue;

  const score = colMap.score ? parseFloat(row[colMap.score]) : null;
  const rank = colMap.rank ? parseInt(row[colMap.rank]) : null;
  const pct = colMap.pct ? parseFloat(row[colMap.pct]) : null;

  imdByLA[code] = {
    score: score && !isNaN(score) ? Math.round(score * 10) / 10 : null,
    rank: rank && !isNaN(rank) ? rank : null,
    pct: pct && !isNaN(pct) ? Math.round(pct * 100 * 10) / 10 : null,
  };
}

console.log(`Built IMD data for ${Object.keys(imdByLA).length} LAs`);

// Map outcode → IMD using LA code cache
const laCodeCachePath = join(RAW, 'outcode-la-code-cache.json');
let laCodeMap = {};

if (existsSync(laCodeCachePath)) {
  laCodeMap = JSON.parse(readFileSync(laCodeCachePath, 'utf-8'));
  console.log(`Loaded ${Object.keys(laCodeMap).length} outcode→LA code entries`);
} else {
  // Need to build it — fetch from postcodes.io
  console.log('Building outcode→LA code cache from postcodes.io...');
  const outcodeCache = JSON.parse(readFileSync(join(RAW, 'outcode-la-cache.json'), 'utf-8'));
  const outcodes = Object.keys(outcodeCache);
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
            laCodeMap[item.query.toUpperCase()] = item.result.admin_district_code;
          }
        }
      }
    } catch (e) {}
    if (i % 500 === 0) process.stdout.write(`${i}/${outcodes.length} `);
    await new Promise(r => setTimeout(r, 80));
  }
  writeFileSync(laCodeCachePath, JSON.stringify(laCodeMap));
  console.log(`Saved ${Object.keys(laCodeMap).length} entries`);
}

// Build final output
const result = {};
let matched = 0;
for (const [outcode, laCode] of Object.entries(laCodeMap)) {
  if (imdByLA[laCode]) {
    result[outcode.toUpperCase()] = imdByLA[laCode];
    matched++;
  }
}

console.log(`Matched ${matched} outcodes to IMD data`);
writeFileSync(OUT, JSON.stringify(result));
console.log(`Written to ${OUT}`);
