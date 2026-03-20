/**
 * Humanisation utilities for postcode data pages.
 * Generates varied, natural-language descriptions from raw data.
 */

/** Simple hash from postcode string to pick sentence variants */
export function postcodeHash(code: string): number {
  let h = 0;
  for (let i = 0; i < code.length; i++) {
    h = ((h << 5) - h + code.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

/** Pick an item from an array using a hash seed */
function pick<T>(arr: T[], hash: number, offset = 0): T {
  return arr[(hash + offset) % arr.length];
}

/** Determine area type from data signals */
export function inferAreaType(data: {
  property: { avgPrice: number; byType: { flat: { count: number }; detached: { count: number }; terraced: { count: number }; semi: { count: number } } };
  demographics: { population: number; ownerOccupied: number };
  broadband: { avgDownload: number };
  crime: { totalPer1000: number };
}): 'urban' | 'suburban' | 'rural' | 'coastal' | 'town' {
  const flatRatio = data.property.byType.flat.count /
    Math.max(1, data.property.byType.flat.count + data.property.byType.detached.count + data.property.byType.terraced.count + data.property.byType.semi.count);
  const detachedRatio = data.property.byType.detached.count /
    Math.max(1, data.property.byType.flat.count + data.property.byType.detached.count + data.property.byType.terraced.count + data.property.byType.semi.count);

  if (flatRatio > 0.45) return 'urban';
  if (detachedRatio > 0.4 && data.demographics.population < 30000) return 'rural';
  if (detachedRatio > 0.35) return 'suburban';
  if (data.demographics.population > 80000) return 'urban';
  return 'town';
}

/** "Living in [Area]" opening paragraph */
export function livingInParagraph(
  code: string,
  name: string,
  areaType: string,
  data: {
    property: { avgPrice: number; priceChange1y: number };
    crime: { vsNational: number };
    schools: { outstanding: number; count: number };
    demographics: { population: number; medianAge: number; ownerOccupied: number };
    broadband: { avgDownload: number };
  }
): string {
  const hash = postcodeHash(code);

  const urbanOpeners = [
    `${name} is a bustling urban area where city life meets community spirit.`,
    `Located in the heart of the action, ${name} offers the convenience and energy that comes with city living.`,
    `Life in ${name} revolves around good transport links, local amenities, and the kind of variety you only find in a city setting.`,
    `${name} is very much a city postcode — expect a mix of flats, shops on your doorstep, and a lively atmosphere.`,
    `If you are after the buzz of urban living, ${name} delivers with its mix of housing, amenities, and connectivity.`,
  ];

  const suburbanOpeners = [
    `${name} strikes a balance between town and country — close enough to commute, green enough to breathe.`,
    `This is suburban living at its most typical: family homes, local schools, and a quieter pace than the city centre.`,
    `${name} is the kind of area where you will find tree-lined streets, decent-sized gardens, and a strong sense of community.`,
    `Residents of ${name} enjoy the best of both worlds — good access to urban amenities without sacrificing space or greenery.`,
    `${name} is a well-established suburban area popular with families and commuters alike.`,
  ];

  const ruralOpeners = [
    `${name} is a quiet, predominantly rural area where open spaces outnumber traffic lights.`,
    `Life in ${name} is defined by countryside views, village communities, and a pace of life that city dwellers often envy.`,
    `If green fields and country lanes appeal more than high streets and tower blocks, ${name} might be your kind of place.`,
    `${name} is a rural postcode district where detached homes, farmland, and tight-knit communities are the norm.`,
    `This is proper countryside — ${name} offers space, privacy, and the kind of peace and quiet that is increasingly hard to find.`,
  ];

  const townOpeners = [
    `${name} is a market town postcode with a character all its own — independent shops, local pubs, and a strong community identity.`,
    `Life in ${name} revolves around a traditional town centre with good local services and a mix of housing.`,
    `${name} is a well-connected town area offering a solid mix of amenities, schooling, and housing options.`,
    `This is a typical English town postcode — ${name} has a bit of everything, from terraced streets to newer estates.`,
    `${name} combines the community feel of a smaller town with reasonable access to larger cities nearby.`,
  ];

  const openerMap: Record<string, string[]> = {
    urban: urbanOpeners,
    suburban: suburbanOpeners,
    rural: ruralOpeners,
    coastal: ruralOpeners, // reuse rural with coastal flavour added below
    town: townOpeners,
  };

  let opener = pick(openerMap[areaType] || townOpeners, hash);

  // Build demographic colour
  const popStr = data.demographics.population > 0
    ? ` With a population of around ${data.demographics.population.toLocaleString()}`
    : '';
  const ageStr = data.demographics.medianAge > 0
    ? ` and a median age of ${data.demographics.medianAge}`
    : '';
  const ownerStr = data.demographics.ownerOccupied > 60
    ? ', this is predominantly an owner-occupied area'
    : data.demographics.ownerOccupied < 40
      ? ', the rental market is significant here'
      : ', there is a healthy mix of owners and renters';

  const demographicSentence = popStr
    ? `${popStr}${ageStr}${ownerStr}.`
    : '';

  // Price context
  const priceTemplates = [
    `Property prices here average around the figures shown below, giving you a clear picture of what to budget.`,
    `The housing market in ${code} tells its own story — scroll down for the full breakdown.`,
    `Whether you are buying, selling, or just curious, the data below shows exactly where ${code} sits in the market.`,
  ];
  const priceLine = pick(priceTemplates, hash, 3);

  return `${opener} ${demographicSentence} ${priceLine}`.replace(/\s{2,}/g, ' ').trim();
}

/** Price comparison phrase — "making it X the national average" */
export function priceComparison(avgPrice: number, nationalAvg: number): string {
  const pct = ((avgPrice - nationalAvg) / nationalAvg) * 100;
  const formatted = `£${Math.round(nationalAvg).toLocaleString()}`;
  if (pct > 40) return `making it significantly above the national average of ${formatted}`;
  if (pct > 15) return `placing it well above the national average of ${formatted}`;
  if (pct > 5) return `slightly above the national average of ${formatted}`;
  if (pct > -5) return `broadly in line with the national average of ${formatted}`;
  if (pct > -15) return `slightly below the national average of ${formatted}`;
  if (pct > -30) return `well below the national average of ${formatted}`;
  return `significantly below the national average of ${formatted}`;
}

/** "What residents say" — characteristics derived from data */
export function residentInsights(
  code: string,
  data: {
    property: { avgPrice: number; byType: { detached: { count: number }; semi: { count: number }; terraced: { count: number }; flat: { count: number } } };
    crime: { vsNational: number; totalPer1000: number };
    schools: { outstanding: number; good: number; count: number };
    demographics: { ownerOccupied: number; medianAge: number; population: number };
    broadband: { avgDownload: number; superfastPct: number };
    councilTax: { bandD: number };
  }
): string[] {
  const insights: string[] = [];
  const total = data.property.byType.detached.count + data.property.byType.semi.count +
    data.property.byType.terraced.count + data.property.byType.flat.count;

  // Housing character
  if (total > 0) {
    const detPct = (data.property.byType.detached.count / total) * 100;
    const flatPct = (data.property.byType.flat.count / total) * 100;
    const terPct = (data.property.byType.terraced.count / total) * 100;
    if (detPct > 40) insights.push('A primarily residential area with spacious detached homes and generous gardens');
    else if (flatPct > 50) insights.push('A flat-dominated area — ideal for young professionals and investors');
    else if (terPct > 40) insights.push('Traditional terraced housing gives the area a strong community character');
    else insights.push('A good mix of property types from flats to family homes');
  }

  // Safety
  if (data.crime.vsNational < -20) insights.push('Residents benefit from crime rates well below the national average');
  else if (data.crime.vsNational < -5) insights.push('Crime levels are below the national average — a reassuring sign for families');
  else if (data.crime.vsNational > 20) insights.push('Crime rates are above average — check street-level data on police.uk for specifics');
  else if (data.crime.vsNational > 5) insights.push('Crime rates are slightly above average — worth researching at street level');
  else insights.push('Crime rates are around the national average');

  // Schools
  if (data.schools.count > 0) {
    const goodPlusPct = ((data.schools.outstanding + data.schools.good) / data.schools.count) * 100;
    if (goodPlusPct > 80) insights.push('Strong school provision — over 80% of local schools rated Good or Outstanding');
    else if (data.schools.outstanding >= 3) insights.push('Multiple Outstanding-rated schools nearby attract families to the area');
    else if (goodPlusPct > 50) insights.push('A reasonable selection of schools, with most rated Good or above');
    else insights.push('School ratings are mixed — research individual schools carefully before buying');
  }

  // Broadband
  if (data.broadband.avgDownload >= 80) insights.push('Excellent broadband speeds make this area ideal for remote workers');
  else if (data.broadband.avgDownload >= 40) insights.push('Broadband speeds are adequate for most households, including streaming and video calls');
  else if (data.broadband.avgDownload < 20) insights.push('Broadband connectivity is limited — check availability at your specific address');

  // Community character
  if (data.demographics.ownerOccupied > 70) insights.push('A settled, owner-occupied community with low tenant turnover');
  else if (data.demographics.ownerOccupied < 35) insights.push('A high proportion of rental properties, giving the area a more transient feel');

  // Council tax
  if (data.councilTax.bandD > 2200) insights.push('Council tax is on the higher side — factor this into your annual budget');
  else if (data.councilTax.bandD < 1400) insights.push('Council tax rates are relatively low compared to much of England');

  return insights.slice(0, 5);
}

/** Varied price commentary templates */
export function priceContextSentence(
  code: string,
  avgPrice: number,
  nationalAvg: number,
  countyAvg: number,
  priceChange1y: number,
  hash: number
): string {
  const vsNat = ((avgPrice - nationalAvg) / nationalAvg) * 100;
  const vsCty = ((avgPrice - countyAvg) / countyAvg) * 100;

  const rising = priceChange1y > 2;
  const falling = priceChange1y < -2;
  const stable = !rising && !falling;

  const trendWord = rising ? 'rising' : falling ? 'falling' : 'broadly stable';
  const trendDetail = rising
    ? `up ${priceChange1y.toFixed(1)}% over the past year`
    : falling
      ? `down ${Math.abs(priceChange1y).toFixed(1)}% over the past year`
      : `with prices moving less than 2% in either direction over the past year`;

  const templates = [
    `At £${Math.round(avgPrice).toLocaleString()}, property in ${code} is ${trendWord} — ${trendDetail}. That puts it ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'above' : 'below'} the England and Wales average.`,
    `The average home in ${code} costs £${Math.round(avgPrice).toLocaleString()}, ${trendDetail}. Compared to the wider county, prices here are ${Math.abs(Math.round(vsCty))}% ${vsCty > 0 ? 'higher' : 'lower'}.`,
    `House prices in ${code} currently sit at £${Math.round(avgPrice).toLocaleString()} on average, ${trendDetail}. For context, the national average is £${Math.round(nationalAvg).toLocaleString()}.`,
    `You will pay around £${Math.round(avgPrice).toLocaleString()} for a property in ${code} — prices have been ${trendWord}, ${trendDetail}.`,
    `${code} has an average property price of £${Math.round(avgPrice).toLocaleString()}. Prices are ${trendWord} here, ${trendDetail}.`,
    `The typical home in ${code} sells for £${Math.round(avgPrice).toLocaleString()}, which is ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'more' : 'less'} than the national figure. The trend is ${trendWord}: ${trendDetail}.`,
    `Property costs £${Math.round(avgPrice).toLocaleString()} on average in ${code}. That is ${Math.abs(Math.round(vsCty))}% ${vsCty > 0 ? 'above' : 'below'} the county average, with prices ${trendWord} over the past 12 months.`,
    `At an average of £${Math.round(avgPrice).toLocaleString()}, ${code} sits ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'above' : 'below'} the national benchmark. Prices have been ${trendWord}: ${trendDetail}.`,
    `Buyers in ${code} should expect to pay around £${Math.round(avgPrice).toLocaleString()} — ${trendDetail}, and ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'above' : 'below'} the national average.`,
    `The average sale price in ${code} is £${Math.round(avgPrice).toLocaleString()}, placing the area ${Math.abs(Math.round(vsCty))}% ${vsCty > 0 ? 'above' : 'below'} its county average. The market is currently ${trendWord}.`,
    `At £${Math.round(avgPrice).toLocaleString()}, homes in ${code} cost ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'more' : 'less'} than the typical English and Welsh property. Over the past year, values have ${rising ? 'climbed' : falling ? 'dipped' : 'held steady'}.`,
    `A home in ${code} will set you back £${Math.round(avgPrice).toLocaleString()} on average. That is ${trendWord} — ${trendDetail} — and ${Math.abs(Math.round(vsNat))}% ${vsNat > 0 ? 'above' : 'below'} the national average of £${Math.round(nationalAvg).toLocaleString()}.`,
  ];

  return pick(templates, hash, 7);
}
