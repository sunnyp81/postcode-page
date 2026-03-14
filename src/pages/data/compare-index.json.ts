import { getAllPostcodeSlugs, getPostcodeData } from '../../utils/data';

export async function GET() {
  const slugs = getAllPostcodeSlugs();
  const index: Record<string, object> = {};

  for (const slug of slugs) {
    const d = getPostcodeData(slug);
    if (!d) continue;
    const goodSchools = d.schools.count > 0
      ? Math.round((d.schools.outstanding + d.schools.good) / d.schools.count * 100)
      : 0;
    index[slug.toUpperCase()] = {
      name: d.name,
      county: d.county,
      region: d.region,
      avgPrice: d.property.avgPrice,
      priceChange1y: d.property.priceChange1y,
      transactions12m: d.property.transactions12m,
      crimePer1000: d.crime.totalPer1000,
      schoolsGoodPct: goodSchools,
      schoolsCount: d.schools.count,
      broadbandAvg: d.broadband.avgDownload,
      superfastPct: d.broadband.superfastPct,
      fullFibrePct: d.broadband.fullFibrePct,
      councilTaxBandD: d.councilTax.bandD,
      ownerOccupied: d.demographics.ownerOccupied,
    };
  }

  return new Response(JSON.stringify(index), {
    headers: { 'Content-Type': 'application/json' },
  });
}
