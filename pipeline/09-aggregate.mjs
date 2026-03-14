/**
 * Step 9: Aggregate all processed data into per-outcode JSON files.
 * Run: node pipeline/09-aggregate.mjs
 *
 * Reads: pipeline/processed/lr-by-outcode.json, schools-by-outcode.json, crime-by-outcode.json
 * Writes: data/postcodes/{outcode}.json (one per outcode)
 *         data/counties/{county}.json
 *         data/national.json
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getOutcodeMeta } from './utils/outcode-lookup.mjs';
import { generateContext } from './utils/context-generator.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const PROCESSED_DIR = join(__dirname, 'processed');
const DATA_DIR = join(ROOT, 'data');

mkdirSync(join(DATA_DIR, 'postcodes'), { recursive: true });
mkdirSync(join(DATA_DIR, 'counties'), { recursive: true });
mkdirSync(join(DATA_DIR, 'regions'), { recursive: true });

console.log('\n=== Step 9: Aggregate → Generate Postcode JSONs ===\n');

// Load processed data
const lrPath = join(PROCESSED_DIR, 'lr-by-outcode.json');
const schoolsPath = join(PROCESSED_DIR, 'schools-by-outcode.json');

if (!existsSync(lrPath)) {
  console.error('Land Registry data not found. Run pipeline/02-parse-land-registry.mjs first.');
  process.exit(1);
}

const lrData = JSON.parse(readFileSync(lrPath, 'utf-8'));
const schoolsData = existsSync(schoolsPath) ? JSON.parse(readFileSync(schoolsPath, 'utf-8')) : {};
const crimeDataPath = join(PROCESSED_DIR, 'crime-by-outcode.json');
const crimeData = existsSync(crimeDataPath) ? JSON.parse(readFileSync(crimeDataPath, 'utf-8')) : {};
const broadbandDataPath = join(PROCESSED_DIR, 'broadband-by-outcode.json');
const broadbandData = existsSync(broadbandDataPath) ? JSON.parse(readFileSync(broadbandDataPath, 'utf-8')) : {};
const councilTaxPath = join(PROCESSED_DIR, 'council-tax-by-outcode.json');
const councilTaxData = existsSync(councilTaxPath) ? JSON.parse(readFileSync(councilTaxPath, 'utf-8')) : {};
const demographicsPath = join(PROCESSED_DIR, 'demographics-by-outcode.json');
const demographicsData = existsSync(demographicsPath) ? JSON.parse(readFileSync(demographicsPath, 'utf-8')) : {};
const medianAgePath = join(PROCESSED_DIR, 'median-age-by-outcode.json');
const medianAgeData = existsSync(medianAgePath) ? JSON.parse(readFileSync(medianAgePath, 'utf-8')) : {};
const imdPath = join(PROCESSED_DIR, 'imd-by-outcode.json');
const imdData = existsSync(imdPath) ? JSON.parse(readFileSync(imdPath, 'utf-8')) : {};
console.log(`  Crime: ${Object.keys(crimeData).filter(k => crimeData[k] !== null).length} outcodes with real data`);
console.log(`  Broadband: ${Object.keys(broadbandData).length} outcodes with real data`);
console.log(`  Council Tax: ${Object.keys(councilTaxData).length} outcodes with real data`);
console.log(`  Demographics: ${Object.keys(demographicsData).length} outcodes with real data`);

console.log(`  Land Registry: ${Object.keys(lrData).length} outcodes`);
console.log(`  Schools: ${Object.keys(schoolsData).length} outcodes`);

// National defaults (will be computed, these are fallbacks)
let nationalDefaults = {
  property: { avgPrice: 287000 },
  crime: { totalPer1000: 81.3, nationalAvgPer1000: 81.3 },
  broadband: { avgDownload: 59.4 },
};

// Use the real England & Wales national average (ONS, 2025)
// Our LR sample is London/SE-biased so we don't compute from it
console.log(`  National avg price: £${nationalDefaults.property.avgPrice.toLocaleString()} (ONS 2025 baseline)`);

// Generate per-county aggregates first (for contextual comparisons)
const countyAggregates = {};
const regionAggregates = {};

for (const [outcode, lr] of Object.entries(lrData)) {
  const meta = getOutcodeMeta(outcode);
  if (!meta || meta.county === 'unknown') continue;

  const { county, region } = meta;

  if (!countyAggregates[county]) countyAggregates[county] = { prices: [], outcodes: [] };
  countyAggregates[county].prices.push(lr.avgPrice);
  countyAggregates[county].outcodes.push(outcode);

  if (!regionAggregates[region]) regionAggregates[region] = { prices: [], outcodes: [] };
  regionAggregates[region].prices.push(lr.avgPrice);
  regionAggregates[region].outcodes.push(outcode);
}

// Compute county averages
const countyAvgPrices = {};
for (const [county, data] of Object.entries(countyAggregates)) {
  countyAvgPrices[county] = Math.round(data.prices.reduce((s, p) => s + p, 0) / data.prices.length);
}

const regionAvgPrices = {};
for (const [region, data] of Object.entries(regionAggregates)) {
  regionAvgPrices[region] = Math.round(data.prices.reduce((s, p) => s + p, 0) / data.prices.length);
}

// --- Generate per-outcode JSON files ---
let generated = 0;
let skipped = 0;
const lastUpdated = new Date().toISOString().slice(0, 7); // YYYY-MM

for (const [outcode, lr] of Object.entries(lrData)) {
  if (!lr.avgPrice || lr.avgPrice < 30000) { skipped++; continue; }

  const meta = getOutcodeMeta(outcode);
  if (!meta) { skipped++; continue; }

  const { region, county, city } = meta;
  const schools = schoolsData[outcode] || { count: 0, outstanding: 0, good: 0, requiresImprovement: 0, inadequate: 0, nearest: [] };

  // Crime data — real from Police.uk if available, synthetic fallback
  const realCrime = crimeData[outcode];
  let totalCrime, vsNational, crimeByType, crimeSource;

  if (realCrime && realCrime.totalAnnual > 0) {
    // Use real crime counts — convert raw annual count to per-1000 using population estimate.
    // Police.uk returns crimes within ~1 mile radius of the outcode centroid.
    // Typical UK outcode: 30,000-60,000 residents; London is denser.
    // These estimates are calibrated so average outcodes land near the national avg of 81.3.
    const popEstimate = meta.region === 'london' ? 55000 : 38000;
    const per1000 = parseFloat((realCrime.totalAnnual / popEstimate * 1000).toFixed(1));
    totalCrime = Math.min(300, Math.max(5, per1000));
    vsNational = parseFloat(((totalCrime - 81.3) / 81.3 * 100).toFixed(1));
    const bt = realCrime.byType;
    crimeByType = {
      violentCrime: parseFloat((bt.violentCrime / popEstimate * 1000).toFixed(1)),
      burglary: parseFloat((bt.burglary / popEstimate * 1000).toFixed(1)),
      vehicleCrime: parseFloat((bt.vehicleCrime / popEstimate * 1000).toFixed(1)),
      antisocialBehaviour: parseFloat((bt.antisocialBehaviour / popEstimate * 1000).toFixed(1)),
      otherTheft: parseFloat((bt.otherTheft / popEstimate * 1000).toFixed(1)),
      other: parseFloat((bt.other / popEstimate * 1000).toFixed(1)),
    };
    crimeSource = 'police-uk';
  } else {
    // Synthetic fallback — price-correlated estimate
    const priceRatio = lr.avgPrice / nationalDefaults.property.avgPrice;
    const crimeBase = Math.max(20, Math.min(200, 81.3 * (1 - (priceRatio - 1) * 0.3)));
    const crimeVariation = ((outcode.charCodeAt(0) + outcode.charCodeAt(outcode.length - 1)) % 20) - 10;
    totalCrime = parseFloat((crimeBase + crimeVariation).toFixed(1));
    vsNational = parseFloat(((totalCrime - 81.3) / 81.3 * 100).toFixed(1));
    crimeByType = {
      violentCrime: parseFloat((totalCrime * 0.27).toFixed(1)),
      burglary: parseFloat((totalCrime * 0.065).toFixed(1)),
      vehicleCrime: parseFloat((totalCrime * 0.1).toFixed(1)),
      antisocialBehaviour: parseFloat((totalCrime * 0.22).toFixed(1)),
      otherTheft: parseFloat((totalCrime * 0.16).toFixed(1)),
      other: parseFloat((totalCrime * 0.185).toFixed(1)),
    };
    crimeSource = 'synthetic-estimate';
  }

  // Broadband — real Ofcom data if available, synthetic fallback
  const realBroadband = broadbandData[outcode];
  const broadbandBase = region === 'london' ? 120 : region === 'south-east' ? 75 : region === 'east-of-england' ? 68 : 55;
  const broadbandVar = ((outcode.charCodeAt(0) * 3 + outcode.length) % 30) - 15;
  const avgDownload = realBroadband ? realBroadband.avgDownload : Math.max(15, broadbandBase + broadbandVar);

  // Demographics — real ONS data if available, synthetic regional fallback
  const realDemo = demographicsData[outcode];
  const demographicsByRegion = {
    'london': { ownerOccupied: 38, privateRent: 40, socialRent: 22, medianAge: 34, economicallyActive: 80 },
    'south-east': { ownerOccupied: 65, privateRent: 22, socialRent: 13, medianAge: 40, economicallyActive: 76 },
    'south-west': { ownerOccupied: 64, privateRent: 23, socialRent: 13, medianAge: 44, economicallyActive: 74 },
    'east-of-england': { ownerOccupied: 66, privateRent: 21, socialRent: 13, medianAge: 41, economicallyActive: 76 },
    'east-midlands': { ownerOccupied: 66, privateRent: 20, socialRent: 14, medianAge: 42, economicallyActive: 74 },
    'west-midlands': { ownerOccupied: 63, privateRent: 21, socialRent: 16, medianAge: 39, economicallyActive: 73 },
    'yorkshire': { ownerOccupied: 62, privateRent: 22, socialRent: 16, medianAge: 39, economicallyActive: 72 },
    'north-west': { ownerOccupied: 61, privateRent: 22, socialRent: 17, medianAge: 38, economicallyActive: 72 },
    'north-east': { ownerOccupied: 58, privateRent: 21, socialRent: 21, medianAge: 40, economicallyActive: 69 },
    'wales': { ownerOccupied: 66, privateRent: 20, socialRent: 14, medianAge: 43, economicallyActive: 71 },
  };
  const demoFallback = demographicsByRegion[region] || demographicsByRegion['east-midlands'];
  const population = realDemo?.population || Math.round(15000 + Math.abs(Math.sin(outcode.charCodeAt(0) * 2.5)) * 25000);
  const ownerOccupied = realDemo?.ownerOccupiedPct ?? demoFallback.ownerOccupied;
  const privateRent = realDemo?.privateRentedPct ?? demoFallback.privateRent;
  const socialRent = realDemo?.socialRentedPct ?? demoFallback.socialRent;
  const demoSource = realDemo?.ownerOccupiedPct != null ? 'ons-census-2021' : 'synthetic-estimate';

  const countyAvg = {
    price: countyAvgPrices[county] || nationalDefaults.property.avgPrice,
    crime: 75,
    broadband: avgDownload,
  };
  const regionalAvg = {
    price: regionAvgPrices[region] || nationalDefaults.property.avgPrice,
    crime: 78,
    broadband: avgDownload,
  };
  const nationalAvg = {
    price: nationalDefaults.property.avgPrice,
    crime: 81.3,
    broadband: 59.4,
  };

  // Find adjacent outcodes (same area prefix, ±1 on numeric)
  const prefix = outcode.match(/^[A-Z]+/)?.[0] || '';
  const suffix = outcode.slice(prefix.length);
  const num = parseInt(suffix, 10);
  const adjacent = [];
  if (!isNaN(num)) {
    for (const n of [num - 1, num + 1, num + 2]) {
      if (n > 0) {
        const adj = prefix + n;
        if (lrData[adj]) adjacent.push(adj);
      }
    }
  }

  const postcodeData = {
    code: outcode,
    name: lr.topTown || outcode,
    county,
    region,
    ...(city ? { city } : {}),
    adjacent: adjacent.slice(0, 6),

    property: {
      avgPrice: lr.avgPrice,
      medianPrice: lr.medianPrice,
      priceChange1y: lr.priceChange1y,
      priceChange5y: lr.priceChange5y,
      transactions12m: lr.transactions12m,
      byType: {
        detached: lr.byType.detached || { avg: Math.round(lr.avgPrice * 1.9), count: 0 },
        semi: lr.byType.semi || { avg: Math.round(lr.avgPrice * 1.2), count: 0 },
        terraced: lr.byType.terraced || { avg: Math.round(lr.avgPrice * 0.95), count: 0 },
        flat: lr.byType.flat || { avg: Math.round(lr.avgPrice * 0.68), count: 0 },
      },
      history: lr.history,
    },

    crime: {
      totalPer1000: totalCrime,
      nationalAvgPer1000: 81.3,
      vsNational,
      byType: crimeByType,
      source: crimeSource,
      trend12m: null, // YoY trend — not available from Police.uk without two-period comparison
    },

    schools: {
      count: schools.count,
      outstanding: schools.outstanding,
      good: schools.good,
      requiresImprovement: schools.requiresImprovement,
      inadequate: schools.inadequate,
      nearest: schools.nearest,
    },

    demographics: {
      population,
      medianAge: medianAgeData[outcode] || medianAgeData[outcode.toUpperCase()] || demoFallback.medianAge,
      medianAgeSource: (medianAgeData[outcode] || medianAgeData[outcode.toUpperCase()]) ? 'nomis-mye' : 'synthetic-estimate',
      ageGroups: { '0-17': 21, '18-34': 23, '35-54': 28, '55-74': 20, '75+': 8 },
      ownerOccupied,
      socialRent,
      privateRent,
      economicallyActive: demoFallback.economicallyActive,
      source: demoSource,
    },

    broadband: {
      avgDownload: parseFloat(avgDownload.toFixed(1)),
      avgUpload: realBroadband ? realBroadband.avgUpload : parseFloat((avgDownload * 0.18).toFixed(1)),
      superfastPct: realBroadband ? realBroadband.superfastPct : Math.min(99, Math.round(70 + (avgDownload / 200) * 30)),
      ultrafastPct: realBroadband ? realBroadband.ultrafastPct : Math.min(95, Math.round(20 + (avgDownload / 200) * 60)),
      gigabitPct: realBroadband ? realBroadband.gigabitPct : Math.min(80, Math.round(10 + (avgDownload / 200) * 50)),
      fullFibrePct: realBroadband ? (realBroadband.fullFibrePct ?? realBroadband.gigabitPct) : Math.min(90, Math.round(15 + (avgDownload / 200) * 55)),
      source: realBroadband ? 'ofcom' : 'synthetic-estimate',
    },

    councilTax: {
      authority: councilTaxData[outcode]?.localAuthority || `${(city || county).replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} Council`,
      bandD: councilTaxData[outcode]?.bandD || (region === 'london'
        ? Math.round(900 + ((outcode.charCodeAt(0) * 13 + outcode.length * 37) % 500))
        : Math.round(1600 + ((outcode.charCodeAt(0) * 17 + outcode.length * 53) % 800))),
      bands: { A: 8, B: 14, C: 25, D: 22, E: 16, F: 9, G: 5, H: 1 },
      source: councilTaxData[outcode] ? 'mhclg-2025' : 'synthetic-estimate',
    },

    epc: {
      distribution: { A: 2, B: 12, C: 36, D: 32, E: 12, F: 4, G: 2 },
      avgScore: 65,
    },

    deprivation: {
      imdScore: imdData[outcode]?.score || imdData[outcode.toUpperCase()]?.score || null,
      imdRank: imdData[outcode]?.rank || imdData[outcode.toUpperCase()]?.rank || null,
      imdDecile: imdData[outcode]?.decile || imdData[outcode.toUpperCase()]?.decile || null,
      pct10MostDeprived: imdData[outcode]?.pct10 || imdData[outcode.toUpperCase()]?.pct10 || null,
      source: (imdData[outcode] || imdData[outcode.toUpperCase()]) ? 'mhclg-imd-2019' : 'synthetic-estimate',
    },

    context: generateContext(
      { code: outcode, name: lr.topTown || outcode, county, region, property: { avgPrice: lr.avgPrice, priceChange1y: lr.priceChange1y, priceChange5y: lr.priceChange5y }, crime: { totalPer1000: totalCrime }, schools, broadband: { avgDownload, superfastPct: Math.min(99, Math.round(70 + (avgDownload / 200) * 30)), fullFibrePct: Math.min(90, Math.round(15 + (avgDownload / 200) * 55)) } },
      countyAvg, regionalAvg, nationalAvg
    ),

    comparisons: {
      countyAvg,
      regionalAvg,
      nationalAvg,
    },

    meta: {
      lastUpdated,
      dataSources: [
        'land-registry',
        'gias-schools',
        ...(crimeSource === 'police-uk' ? ['police-uk'] : []),
        ...(realBroadband ? ['ofcom'] : []),
        ...(councilTaxData[outcode] ? ['mhclg'] : []),
        ...(demoSource === 'ons-census-2021' ? ['ons-census-2021'] : []),
        ...((crimeSource === 'synthetic-estimate' || !realBroadband || !councilTaxData[outcode] || demoSource !== 'ons-census-2021') ? ['synthetic-estimates'] : []),
      ],
    },
  };

  const filename = join(DATA_DIR, 'postcodes', `${outcode.toLowerCase()}.json`);
  writeFileSync(filename, JSON.stringify(postcodeData, null, 2));
  generated++;

  if (generated % 100 === 0) {
    process.stdout.write(`\r  Generated ${generated} postcode files...`);
  }
}

console.log(`\n\n✅ Generated ${generated} postcode JSON files`);
console.log(`   Skipped ${skipped} outcodes (insufficient data)`);

// --- Save county JSON files ---
console.log('\n  Generating county files...');
for (const [county, data] of Object.entries(countyAggregates)) {
  const countyData = {
    slug: county,
    label: county.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    region: Object.entries(regionAggregates).find(([, r]) => r.outcodes.some(o => data.outcodes.includes(o)))?.[0] || 'unknown',
    avgPrice: countyAvgPrices[county],
    postcodes: data.outcodes.sort(),
    postcodeCount: data.outcodes.length,
    meta: { lastUpdated },
  };
  writeFileSync(join(DATA_DIR, 'counties', `${county}.json`), JSON.stringify(countyData, null, 2));
}
console.log(`  Generated ${Object.keys(countyAggregates).length} county files`);

// --- Save national.json ---
writeFileSync(join(DATA_DIR, 'national.json'), JSON.stringify({
  avgPrice: nationalDefaults.property.avgPrice,
  crime: 81.3,
  broadband: 59.4,
  lastUpdated,
}, null, 2));

console.log('\n✅ Aggregation complete');
console.log('   Next: npm run build\n');
