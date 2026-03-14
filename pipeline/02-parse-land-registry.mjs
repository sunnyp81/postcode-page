/**
 * Step 2: Parse Land Registry Price Paid CSVs into per-outcode aggregates.
 * Run: node pipeline/02-parse-land-registry.mjs
 *
 * Input:  pipeline/raw/lr-YYYY-MM.csv (multiple files)
 * Output: pipeline/processed/lr-by-outcode.json
 *
 * LR CSV columns (no header):
 * 0:  transaction_id
 * 1:  price
 * 2:  date (YYYY-MM-DD HH:MM)
 * 3:  postcode
 * 4:  property_type (D/S/T/F/O)
 * 5:  new_build (Y/N)
 * 6:  tenure (F/L/U)
 * 7:  paon (primary address)
 * 8:  saon (secondary address)
 * 9:  street
 * 10: locality
 * 11: town_city
 * 12: district
 * 13: county
 * 14: ppd_category
 * 15: record_status
 */

import { readFileSync, readdirSync, existsSync, writeFileSync, mkdirSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
const PROCESSED_DIR = join(__dirname, 'processed');
mkdirSync(PROCESSED_DIR, { recursive: true });

const TYPE_MAP = { D: 'detached', S: 'semi', T: 'terraced', F: 'flat', O: 'other' };

// Get outcode from full postcode: "GU1 1AA" → "GU1", "SW1A 2AA" → "SW1A"
function toOutcode(postcode) {
  if (!postcode) return null;
  const parts = postcode.trim().split(' ');
  return parts[0]?.toUpperCase() || null;
}

// Median of sorted array
function median(sorted) {
  if (!sorted.length) return 0;
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : Math.round((sorted[mid - 1] + sorted[mid]) / 2);
}

console.log('\n=== Step 2: Parse Land Registry Price Paid ===\n');

// Read the combined LR file
const lrCombined = join(RAW_DIR, 'lr-combined.csv');
if (!existsSync(lrCombined)) {
  console.error('lr-combined.csv not found. Run pipeline/01-download.mjs first.');
  process.exit(1);
}

const sizeMB = (statSync(lrCombined).size / 1024 / 1024).toFixed(1);
console.log(`  Reading lr-combined.csv (${sizeMB}MB)...`);

// Structure: outcode → { prices[], byType: { D: [], S: [], T: [], F: [] }, towns: {}, yearPrices: { 2024: [], 2023: [] } }
const byOutcode = new Map();

let totalRows = 0;
let skipped = 0;

{
  const filepath = lrCombined;
  const content = readFileSync(filepath, 'utf-8');
  const lines = content.split('\n');

  for (const line of lines) {
    if (!line.trim()) continue;

    // Parse CSV row (simple split — LR data is clean)
    const cols = line.split(',').map(c => c.replace(/^"|"$/g, ''));
    if (cols.length < 14) { skipped++; continue; }

    const price = parseInt(cols[1], 10);
    const dateStr = cols[2]?.slice(0, 10); // YYYY-MM-DD
    const postcode = cols[3];
    const propType = cols[4];
    const townCity = cols[11]?.trim();
    const district = cols[12]?.trim();
    const county = cols[13]?.trim();

    if (!price || price < 10000 || price > 50000000) { skipped++; continue; }
    if (!postcode) { skipped++; continue; }

    const outcode = toOutcode(postcode);
    if (!outcode) { skipped++; continue; }

    const year = dateStr ? parseInt(dateStr.slice(0, 4), 10) : null;
    if (!year || year < 2019) { skipped++; continue; }

    if (!byOutcode.has(outcode)) {
      byOutcode.set(outcode, {
        prices: [],
        byType: { D: [], S: [], T: [], F: [] },
        yearPrices: {},
        towns: {},
        districts: {},
        counties: {},
      });
    }

    const entry = byOutcode.get(outcode);
    entry.prices.push(price);

    if (TYPE_MAP[propType] && propType !== 'O') {
      entry.byType[propType].push(price);
    }

    if (year) {
      if (!entry.yearPrices[year]) entry.yearPrices[year] = [];
      entry.yearPrices[year].push(price);
    }

    if (townCity) entry.towns[townCity] = (entry.towns[townCity] || 0) + 1;
    if (district) entry.districts[district] = (entry.districts[district] || 0) + 1;
    if (county) entry.counties[county] = (entry.counties[county] || 0) + 1;

    totalRows++;
  }
} // end file block

console.log(`\n  Parsed ${totalRows.toLocaleString()} transactions across ${byOutcode.size} outcodes`);
console.log(`  Skipped ${skipped.toLocaleString()} invalid rows`);

// Aggregate per outcode
console.log('\n  Aggregating...');
const aggregated = {};
const currentYear = new Date().getFullYear();

for (const [outcode, data] of byOutcode) {
  if (data.prices.length < 3) continue; // Skip outcodes with very few transactions

  const sorted = [...data.prices].sort((a, b) => a - b);
  const avgPrice = Math.round(sorted.reduce((s, p) => s + p, 0) / sorted.length);
  const medianPrice = median(sorted);

  // By type averages
  const byType = {};
  for (const [typeCode, typeName] of Object.entries(TYPE_MAP)) {
    if (typeCode === 'O') continue;
    const prices = data.byType[typeCode];
    if (prices.length > 0) {
      const avg = Math.round(prices.reduce((s, p) => s + p, 0) / prices.length);
      byType[typeName] = { avg, count: prices.length };
    } else {
      // Use a derived estimate if no data
      const multipliers = { detached: 1.9, semi: 1.2, terraced: 1.0, flat: 0.68 };
      const baseType = 'terraced';
      if (byType[baseType]) {
        byType[typeName] = { avg: Math.round(byType[baseType].avg * multipliers[typeName] / multipliers[baseType]), count: 0 };
      } else {
        byType[typeName] = { avg: Math.round(avgPrice * (multipliers[typeName] || 1)), count: 0 };
      }
    }
  }

  // Year-on-year history (last 6 years)
  const history = [];
  for (let y = currentYear - 5; y <= currentYear; y++) {
    const yearPrices = data.yearPrices[y];
    if (yearPrices && yearPrices.length >= 2) {
      const yearAvg = Math.round(yearPrices.reduce((s, p) => s + p, 0) / yearPrices.length);
      history.push({ year: y, avg: yearAvg });
    }
  }

  // YoY change
  let priceChange1y = 0;
  let priceChange5y = 0;
  if (history.length >= 2) {
    const last = history[history.length - 1];
    const prev = history[history.length - 2];
    priceChange1y = parseFloat(((last.avg - prev.avg) / prev.avg * 100).toFixed(1));

    const fiveYearsAgo = history.find(h => h.year <= currentYear - 4);
    if (fiveYearsAgo) {
      priceChange5y = parseFloat(((last.avg - fiveYearsAgo.avg) / fiveYearsAgo.avg * 100).toFixed(1));
    }
  }

  // Most common town name (for area name)
  const topTown = Object.entries(data.towns).sort((a, b) => b[1] - a[1])[0]?.[0] || outcode;
  const topCounty = Object.entries(data.counties).sort((a, b) => b[1] - a[1])[0]?.[0] || '';

  aggregated[outcode] = {
    avgPrice,
    medianPrice,
    priceChange1y,
    priceChange5y,
    transactions12m: sorted.length, // approximate (all files loaded are ~18 months, use proportional)
    byType,
    history: history.length >= 2 ? history : [{ year: currentYear, avg: avgPrice }],
    topTown,
    topCounty,
  };
}

const outputPath = join(PROCESSED_DIR, 'lr-by-outcode.json');
writeFileSync(outputPath, JSON.stringify(aggregated, null, 2));
console.log(`\n✅ Land Registry processing complete`);
console.log(`   ${Object.keys(aggregated).length} outcodes aggregated`);
console.log(`   Output: pipeline/processed/lr-by-outcode.json\n`);
console.log('   Next: node pipeline/04-parse-schools.mjs\n');
