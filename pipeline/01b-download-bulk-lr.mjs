/**
 * Step 1b: Download Land Registry Price Paid bulk CSVs (2024 + 2025).
 * Covers all ~3,000 UK postcode districts — much better than API sampling.
 *
 * Run: node pipeline/01b-download-bulk-lr.mjs
 * Output: pipeline/raw/lr-combined.csv (replaced with full UK data)
 *
 * Files: ~500MB each year, ~1GB combined
 * Time: 5-15 min depending on connection
 */

import { existsSync, writeFileSync, appendFileSync, mkdirSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
mkdirSync(RAW_DIR, { recursive: true });

const OUT = join(RAW_DIR, 'lr-combined.csv');

const YEARS = ['2023', '2024', '2025'];
const BASE_URL = 'https://price-paid-data.publicdata.landregistry.gov.uk';

console.log('\n=== Step 1b: Download LR Bulk CSVs ===\n');
console.log(`  Years: ${YEARS.join(', ')}`);
console.log(`  Output: lr-combined.csv\n`);

// Clear output file
writeFileSync(OUT, '');

let totalRows = 0;

for (const year of YEARS) {
  const url = `${BASE_URL}/pp-${year}.csv`;  // e.g. pp-2024.csv, pp-2025.csv
  console.log(`  📥 Downloading ${year} (${url})...`);

  const res = await fetch(url, {
    headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
    signal: AbortSignal.timeout(300000), // 5 min timeout per file
  });

  if (!res.ok) {
    console.warn(`  ⚠ ${year} failed: ${res.status} — skipping`);
    continue;
  }

  // Stream the response to file
  const reader = res.body.getReader();
  let chunk = '';
  let bytesRead = 0;
  let rowsInFile = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const text = new TextDecoder().decode(value);
    chunk += text;
    bytesRead += value.length;

    // Split on newlines, keep last partial line
    const lines = chunk.split('\n');
    chunk = lines.pop() || '';

    const validLines = lines.filter(l => l.trim() && l.split(',').length >= 14);
    if (validLines.length) {
      appendFileSync(OUT, validLines.join('\n') + '\n');
      rowsInFile += validLines.length;
    }

    if (rowsInFile % 100000 === 0 && rowsInFile > 0) {
      process.stdout.write(`\r    ${(bytesRead / 1024 / 1024).toFixed(0)}MB | ${rowsInFile.toLocaleString()} rows`);
    }
  }

  // Flush remaining chunk
  if (chunk.trim()) {
    appendFileSync(OUT, chunk + '\n');
    rowsInFile++;
  }

  totalRows += rowsInFile;
  const mb = (statSync(OUT).size / 1024 / 1024).toFixed(0);
  console.log(`\n  ✓ ${year}: ${rowsInFile.toLocaleString()} rows | Total file: ${mb}MB`);
}

const finalMB = (statSync(OUT).size / 1024 / 1024).toFixed(0);
console.log(`\n✅ Done: ${totalRows.toLocaleString()} total rows | ${finalMB}MB`);
console.log('   Next: node pipeline/02-parse-land-registry.mjs\n');
