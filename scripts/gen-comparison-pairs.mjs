import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, '..', 'data', 'postcodes');
const files = readdirSync(dir);
const pairs = new Set();

for (const f of files) {
  const d = JSON.parse(readFileSync(join(dir, f), 'utf8'));
  const a = (d.code || f.replace('.json','')).toUpperCase();
  for (const b of (d.adjacent || [])) {
    if (b && b.toUpperCase() !== a) {
      const pair = [a, b.toUpperCase()].sort().join('-vs-');
      pairs.add(pair);
    }
  }
}

console.log('Total pairs:', pairs.size);
writeFileSync(join(__dirname, '..', 'data', 'comparison-pairs.json'), JSON.stringify([...pairs]));
console.log('Written to data/comparison-pairs.json');
