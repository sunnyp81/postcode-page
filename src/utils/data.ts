// Data loading utilities — reads pre-processed JSON from the data/ directory
// In Astro, these run at build time (Node.js context)

import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const DATA_DIR = join(process.cwd(), 'data');

export interface PostcodeData {
  code: string;
  name: string;
  county: string;
  region: string;
  city?: string;
  coordinates?: { lat: number; lng: number };
  adjacent: string[];

  property: {
    avgPrice: number;
    medianPrice: number;
    priceChange1y: number;
    priceChange5y: number;
    transactions12m: number;
    byType: {
      detached: { avg: number; count: number };
      semi: { avg: number; count: number };
      terraced: { avg: number; count: number };
      flat: { avg: number; count: number };
    };
    history: Array<{ year: number; avg: number }>;
  };

  crime: {
    totalPer1000: number;
    nationalAvgPer1000: number;
    vsNational: number;
    byType: {
      violentCrime: number;
      burglary: number;
      vehicleCrime: number;
      antisocialBehaviour: number;
      otherTheft: number;
      other: number;
    };
    trend12m: number;
  };

  schools: {
    count: number;
    outstanding: number;
    good: number;
    requiresImprovement: number;
    inadequate: number;
    nearest: Array<{
      name: string;
      type: string;
      ofsted: string;
      distance: number;
    }>;
  };

  demographics: {
    population: number;
    medianAge: number;
    ageGroups: { '0-17': number; '18-34': number; '35-54': number; '55-74': number; '75+': number };
    ownerOccupied: number;
    socialRent: number;
    privateRent: number;
    economicallyActive: number;
  };

  broadband: {
    avgDownload: number;
    avgUpload: number;
    superfastPct: number;
    ultrafastPct: number;
    fullFibrePct: number;
  };

  councilTax: {
    authority: string;
    bandD: number;
    bands: Record<string, number>;
  };

  epc: {
    distribution: Record<string, number>;
    avgScore: number;
  };

  deprivation: {
    imdRank: number;
    imdDecile: number;
    incomeRank: number;
    employmentRank: number;
  };

  context: {
    intro: string;
    priceVsCounty: string;
    priceVsNational: string;
    priceTrend: string;
    crimeContext: string;
    schoolContext: string;
    broadbandContext: string;
    synthesis: string;
  };

  comparisons: {
    countyAvg: { price: number; crime: number; broadband: number };
    regionalAvg: { price: number; crime: number; broadband: number };
    nationalAvg: { price: number; crime: number; broadband: number };
  };

  meta: {
    lastUpdated: string;
    dataSources: string[];
  };
}

export function getAllPostcodeSlugs(): string[] {
  const dir = join(DATA_DIR, 'postcodes');
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter(f => f.endsWith('.json'))
    .map(f => f.replace('.json', ''));
}

export function getPostcodeData(slug: string): PostcodeData | null {
  const filePath = join(DATA_DIR, 'postcodes', `${slug.toLowerCase()}.json`);
  if (!existsSync(filePath)) return null;
  try {
    return JSON.parse(readFileSync(filePath, 'utf-8')) as PostcodeData;
  } catch {
    return null;
  }
}

export function getAdjacentPostcodes(adjacent: string[]): Array<{
  code: string; name: string; avgPrice: number; priceChange1y: number;
}> {
  return adjacent
    .map(code => {
      const data = getPostcodeData(code.toLowerCase());
      if (!data) return null;
      return {
        code: data.code,
        name: data.name,
        avgPrice: data.property.avgPrice,
        priceChange1y: data.property.priceChange1y,
      };
    })
    .filter(Boolean) as Array<{ code: string; name: string; avgPrice: number; priceChange1y: number }>;
}

export function getAllCountySlugs(): string[] {
  const dir = join(DATA_DIR, 'counties');
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter(f => f.endsWith('.json'))
    .map(f => f.replace('.json', ''));
}

export function getCountyData(slug: string) {
  const filePath = join(DATA_DIR, 'counties', `${slug.toLowerCase()}.json`);
  if (!existsSync(filePath)) return null;
  try {
    return JSON.parse(readFileSync(filePath, 'utf-8'));
  } catch {
    return null;
  }
}

export interface CountySummary {
  slug: string;
  label: string;
  region: string;
  avgPrice: number;
  postcodeCount: number;
}

export interface RegionSummary {
  slug: string;
  label: string;
  counties: CountySummary[];
  totalPostcodes: number;
  avgPrice: number;
}

const REGION_LABELS: Record<string, string> = {
  'london': 'London',
  'south-east': 'South East',
  'south-west': 'South West',
  'east-of-england': 'East of England',
  'east-midlands': 'East Midlands',
  'west-midlands': 'West Midlands',
  'yorkshire': 'Yorkshire & the Humber',
  'north-west': 'North West',
  'north-east': 'North East',
  'wales': 'Wales',
};

export function getAllCountyData(): CountySummary[] {
  const slugs = getAllCountySlugs();
  return slugs
    .map(slug => getCountyData(slug))
    .filter(Boolean)
    .map(d => ({
      slug: d.slug,
      label: d.label,
      region: d.region,
      avgPrice: d.avgPrice,
      postcodeCount: d.postcodeCount,
    }));
}

export function getAllRegions(): RegionSummary[] {
  const counties = getAllCountyData();
  const byRegion = new Map<string, CountySummary[]>();

  for (const county of counties) {
    if (!byRegion.has(county.region)) byRegion.set(county.region, []);
    byRegion.get(county.region)!.push(county);
  }

  return Array.from(byRegion.entries())
    .map(([slug, regionCounties]) => {
      const totalPostcodes = regionCounties.reduce((s, c) => s + c.postcodeCount, 0);
      const avgPrice = Math.round(
        regionCounties.reduce((s, c) => s + c.avgPrice * c.postcodeCount, 0) / totalPostcodes
      );
      return {
        slug,
        label: REGION_LABELS[slug] || slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        counties: regionCounties.sort((a, b) => b.avgPrice - a.avgPrice),
        totalPostcodes,
        avgPrice,
      };
    })
    .sort((a, b) => b.avgPrice - a.avgPrice);
}

export function getRegionData(slug: string): RegionSummary | null {
  const regions = getAllRegions();
  return regions.find(r => r.slug === slug) || null;
}

export function getCountyPostcodes(countySlug: string): Array<{
  code: string; name: string; avgPrice: number; priceChange1y: number; crime: number; schools: number;
}> {
  const county = getCountyData(countySlug);
  if (!county) return [];
  return (county.postcodes as string[])
    .map(code => {
      const d = getPostcodeData(code.toLowerCase());
      if (!d) return null;
      return {
        code: d.code,
        name: d.name,
        avgPrice: d.property.avgPrice,
        priceChange1y: d.property.priceChange1y,
        crime: d.crime.totalPer1000,
        schools: d.schools.count,
      };
    })
    .filter(Boolean) as Array<{ code: string; name: string; avgPrice: number; priceChange1y: number; crime: number; schools: number }>;
}
