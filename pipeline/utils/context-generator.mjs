/**
 * Generates unique contextual sentences for each postcode district.
 * Every postcode gets a different combination based on its actual data.
 */

function formatPrice(n) {
  return '£' + Math.round(n).toLocaleString('en-GB');
}
function formatPct(n, decimals = 1) {
  return Math.abs(n).toFixed(decimals) + '%';
}
function aboveBelow(n) { return n >= 0 ? 'above' : 'below'; }
function moreOrLess(n) { return n >= 0 ? 'more' : 'less'; }
function higherLower(n) { return n >= 0 ? 'higher' : 'lower'; }

export function generateContext(data, countyAvg, regionalAvg, nationalAvg) {
  const { code, name, county, region, property, crime, schools, broadband } = data;

  const countyLabel = county.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  const regionLabel = region.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  const priceVsCounty = ((property.avgPrice - countyAvg.price) / countyAvg.price) * 100;
  const priceVsNational = ((property.avgPrice - nationalAvg.price) / nationalAvg.price) * 100;
  const crimeVsNational = ((crime.totalPer1000 - nationalAvg.crime) / nationalAvg.crime) * 100;

  // --- INTRO ---
  const introVariants = [
    `${code} covers ${name} in ${countyLabel}, ${regionLabel}.`,
    `${code} is the postcode district for ${name}, located in ${countyLabel}, ${regionLabel}.`,
    `${code} encompasses ${name} and its surrounding areas in ${countyLabel}, ${regionLabel}.`,
  ];
  const intro = introVariants[Math.abs(hashCode(code)) % introVariants.length];

  // --- PRICE VS COUNTY ---
  const priceVsCountySentence = (() => {
    const diff = Math.abs(priceVsCounty).toFixed(0);
    const dir = aboveBelow(priceVsCounty);
    if (Math.abs(priceVsCounty) < 5) {
      return `The average property price in ${code} is ${formatPrice(property.avgPrice)}, broadly in line with the ${countyLabel} average of ${formatPrice(countyAvg.price)}.`;
    }
    return `The average property price in ${code} is ${formatPrice(property.avgPrice)}, which is ${diff}% ${dir} the ${countyLabel} average of ${formatPrice(countyAvg.price)}.`;
  })();

  // --- PRICE VS NATIONAL ---
  const priceVsNationalSentence = (() => {
    const diff = Math.abs(priceVsNational).toFixed(0);
    if (Math.abs(priceVsNational) < 5) {
      return `Property values in ${code} are close to the England and Wales national average of ${formatPrice(nationalAvg.price)}.`;
    }
    return `Properties in ${code} cost ${formatPct(priceVsNational)} ${moreOrLess(priceVsNational)} than the England and Wales average of ${formatPrice(nationalAvg.price)}.`;
  })();

  // --- PRICE TREND ---
  const priceTrendSentence = (() => {
    const yr = property.priceChange1y;
    const fiveyr = property.priceChange5y;
    const dirYr = yr >= 0 ? 'risen' : 'fallen';
    const dirFive = fiveyr >= 0 ? 'risen' : 'fallen';
    return `House prices in ${code} have ${dirYr} ${formatPct(yr)} over the past 12 months and ${dirFive} ${formatPct(fiveyr)} over five years.`;
  })();

  // --- CRIME ---
  const crimeContext = (() => {
    const diff = Math.abs(crimeVsNational).toFixed(0);
    const dir = aboveBelow(crimeVsNational);
    const adjective = crimeVsNational < -20 ? 'well ' : crimeVsNational < -5 ? '' : crimeVsNational > 20 ? 'significantly ' : '';
    if (Math.abs(crimeVsNational) < 5) {
      return `${code} has a crime rate of ${crime.totalPer1000.toFixed(1)} per 1,000 residents, broadly in line with the national average of ${nationalAvg.crime.toFixed(1)} per 1,000.`;
    }
    return `With ${crime.totalPer1000.toFixed(1)} recorded crimes per 1,000 residents, ${code} has a crime rate ${adjective}${diff}% ${dir} the national average of ${nationalAvg.crime.toFixed(1)} per 1,000.`;
  })();

  // --- SCHOOLS ---
  const schoolContext = (() => {
    const total = schools.count;
    const outstanding = schools.outstanding;
    const good = schools.good;
    if (total === 0) return `School data for ${code} is not currently available.`;
    if (outstanding === 0) {
      return `${code} has ${total} school${total !== 1 ? 's' : ''} within or near its boundaries, including ${good} rated Good by Ofsted.`;
    }
    return `${code} has ${total} school${total !== 1 ? 's' : ''} within or near its boundaries, including ${outstanding} rated Outstanding${good > 0 ? ` and ${good} rated Good` : ''} by Ofsted.`;
  })();

  // --- BROADBAND ---
  const broadbandContext = (() => {
    const speed = broadband.avgDownload.toFixed(1);
    const superfast = broadband.superfastPct;
    const fibre = broadband.fullFibrePct;
    return `Average broadband download speeds in ${code} are ${speed} Mbps, with ${superfast}% of premises having access to superfast broadband and ${fibre}% with full fibre (FTTP) connectivity.`;
  })();

  // --- SYNTHESIS ---
  const synthesis = (() => {
    const parts = [];

    // Price characterisation
    if (priceVsNational > 20) parts.push(`above-average property prices (${formatPrice(property.avgPrice)}, ${Math.abs(priceVsNational).toFixed(0)}% above national average)`);
    else if (priceVsNational < -15) parts.push(`affordable property prices (${formatPrice(property.avgPrice)}, ${Math.abs(priceVsNational).toFixed(0)}% below national average)`);
    else parts.push(`property prices near the national average (${formatPrice(property.avgPrice)})`);

    // Crime
    if (crimeVsNational < -15) parts.push(`low crime (${Math.abs(crimeVsNational).toFixed(0)}% below national average)`);
    else if (crimeVsNational > 15) parts.push(`higher-than-average crime (${Math.abs(crimeVsNational).toFixed(0)}% above national average)`);

    // Schools
    if (schools.outstanding >= 2) parts.push(`strong school provision (${schools.outstanding} Outstanding-rated school${schools.outstanding !== 1 ? 's' : ''})`);
    else if (schools.count > 0) parts.push(`${schools.count} nearby school${schools.count !== 1 ? 's' : ''}`);

    // Broadband
    if (broadband.avgDownload >= 100) parts.push(`excellent broadband (${broadband.avgDownload.toFixed(0)} Mbps average)`);
    else if (broadband.avgDownload >= 50) parts.push(`good broadband connectivity (${broadband.avgDownload.toFixed(0)} Mbps average)`);

    const list = parts.length > 1
      ? parts.slice(0, -1).join(', ') + ', and ' + parts[parts.length - 1]
      : parts[0] || 'a mix of local amenities';

    return `${code} (${name}) offers ${list}.`;
  })();

  return { intro, priceVsCounty: priceVsCountySentence, priceVsNational: priceVsNationalSentence, priceTrend: priceTrendSentence, crimeContext, schoolContext, broadbandContext, synthesis };
}

function hashCode(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}
