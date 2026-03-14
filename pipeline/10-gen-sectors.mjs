/**
 * pipeline/10-gen-sectors.mjs
 * Aggregate LR combined CSV → sector-level JSON files
 * Sector = district + first char of inward code, e.g. "GU1 3" from "GU1 3BT"
 */
import { createReadStream, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CSV = join(__dirname, 'raw', 'lr-combined.csv');
const OUT = join(__dirname, '..', 'data', 'sectors');

if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

// sector → { byType: { D/S/T/F: [prices] }, byYear: { 2021: [prices], ... } }
const sectors = new Map();

function getSector(postcode) {
  if (!postcode) return null;
  const parts = postcode.trim().split(' ');
  if (parts.length !== 2 || parts[1].length < 3) return null;
  return `${parts[0]} ${parts[1][0]}`;
}

const propTypeMap = { D: 'detached', S: 'semi', T: 'terraced', F: 'flat' };

console.log('Reading LR CSV...');
let lineCount = 0;
let validLines = 0;

const rl = createInterface({ input: createReadStream(CSV), crlfDelay: Infinity });

for await (const line of rl) {
  lineCount++;
  // Strip quotes and split
  const cols = line.replace(/"/g, '').split(',');
  const price = parseInt(cols[1]);
  const dateStr = cols[2]; // "2023-06-14 00:00"
  const postcode = cols[3];
  const propType = cols[4]; // D/S/T/F/O

  if (!price || !postcode || !dateStr) continue;

  const year = parseInt(dateStr.substring(0, 4));
  if (year < 2015) continue; // only last ~10 years

  const sector = getSector(postcode);
  if (!sector) continue;

  if (!sectors.has(sector)) {
    sectors.set(sector, { byType: {}, byYear: {} });
  }
  const s = sectors.get(sector);

  // By type
  const pt = propTypeMap[propType] || 'other';
  if (!s.byType[pt]) s.byType[pt] = [];
  s.byType[pt].push(price);

  // By year
  if (!s.byYear[year]) s.byYear[year] = [];
  s.byYear[year].push(price);

  validLines++;
}

console.log(`Read ${lineCount} lines, ${validLines} valid, ${sectors.size} sectors`);

function avg(arr) {
  if (!arr || arr.length === 0) return 0;
  return Math.round(arr.reduce((s, v) => s + v, 0) / arr.length);
}
function median(arr) {
  if (!arr || arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? Math.round((s[m - 1] + s[m]) / 2) : s[m];
}

let written = 0;
const now = new Date();
const cutoff12m = new Date(now.getFullYear(), now.getMonth() - 12, 1);
const cutoff12mYear = cutoff12m.getFullYear();

for (const [sector, data] of sectors) {
  // Need at least 5 transactions in recent 2 years
  const recentYears = [now.getFullYear(), now.getFullYear() - 1];
  const recentPrices = recentYears.flatMap(y => data.byYear[y] || []);
  if (recentPrices.length < 5) continue;

  // Derive outcode from sector
  const outcode = sector.split(' ')[0].toLowerCase();
  const sectorNum = sector.split(' ')[1];
  const slug = `${outcode}-${sectorNum}`.toLowerCase();

  // Build history array (2015–present)
  const history = [];
  for (let y = 2015; y <= now.getFullYear(); y++) {
    const prices = data.byYear[y];
    if (prices && prices.length >= 3) {
      history.push({ year: y, avg: avg(prices) });
    }
  }

  // Current avg (last 2 complete years)
  const avgPrice = avg(recentPrices);
  const medianPrice = median(recentPrices);

  // 1yr change
  const prev2y = recentYears.slice(1).flatMap(y => data.byYear[y] || []);
  const curr1y = data.byYear[now.getFullYear()] || data.byYear[now.getFullYear() - 1] || [];
  const priceChange1y = prev2y.length >= 5 && curr1y.length >= 5
    ? parseFloat(((avg(curr1y) - avg(prev2y)) / avg(prev2y) * 100).toFixed(1))
    : null;

  // By type
  const byType = {};
  for (const [type, prices] of Object.entries(data.byType)) {
    if (prices.length >= 2) {
      byType[type] = { avg: avg(prices), count: prices.length };
    }
  }

  const transactions = recentPrices.length;

  const out = {
    sector,
    outcode: outcode.toUpperCase(),
    sectorNum,
    slug,
    property: {
      avgPrice,
      medianPrice,
      priceChange1y,
      transactions12m: transactions,
      byType,
      history,
    },
  };

  writeFileSync(join(OUT, `${slug}.json`), JSON.stringify(out));
  written++;
}

console.log(`Written ${written} sector files to data/sectors/`);
