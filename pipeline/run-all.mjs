/**
 * Master pipeline runner — runs all steps in sequence.
 * Usage: node pipeline/run-all.mjs [--skip-download]
 *
 * Steps:
 * 1. Download raw data (skip with --skip-download if already done)
 * 2. Parse Land Registry
 * 3. Fetch Police.uk crime
 * 4. Parse Schools + Ofsted
 * 5. Fetch Ofcom broadband
 * 6. Fetch MHCLG council tax
 * 7. Fetch ONS demographics
 * 9. Aggregate → generate postcode JSONs
 */

import { execSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const skipDownload = process.argv.includes('--skip-download');

function run(script, label) {
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  Running: ${label}`);
  console.log('═'.repeat(50));
  try {
    execSync(`node ${join(__dirname, script)}`, { stdio: 'inherit' });
  } catch (err) {
    console.error(`\n✗ Step failed: ${label}`);
    console.error(err.message);
    process.exit(1);
  }
}

const start = Date.now();

if (!skipDownload) {
  run('01-download.mjs', 'Download raw datasets');
}

run('02-parse-land-registry.mjs', 'Parse Land Registry Price Paid');
run('03-fetch-crime.mjs', 'Fetch Police.uk crime data');
run('04-parse-schools.mjs', 'Parse DfE Schools data');
run('04b-fetch-ofsted.mjs', 'Merge Ofsted inspection grades');
run('05-fetch-broadband.mjs', 'Fetch Ofcom broadband coverage data');
run('06-fetch-council-tax.mjs', 'Fetch MHCLG council tax Band D rates');
run('07-fetch-demographics.mjs', 'Fetch ONS demographics (population, tenure)');
run('09-aggregate.mjs', 'Aggregate → generate postcode JSONs');

const elapsed = ((Date.now() - start) / 1000).toFixed(1);
console.log(`\n${'═'.repeat(50)}`);
console.log(`  ✅ Pipeline complete in ${elapsed}s`);
console.log(`  Next: npm run build && wrangler pages deploy dist/`);
console.log('═'.repeat(50) + '\n');
