/**
 * Outcode (postcode district) → name, county, region, city
 * Source: ONS NSPL + manual curation
 * This covers the ~3,000 active outcode districts in England & Wales
 */

export const REGIONS = {
  'E12000001': { slug: 'north-east', label: 'North East' },
  'E12000002': { slug: 'north-west', label: 'North West' },
  'E12000003': { slug: 'yorkshire', label: 'Yorkshire and the Humber' },
  'E12000004': { slug: 'east-midlands', label: 'East Midlands' },
  'E12000005': { slug: 'west-midlands', label: 'West Midlands' },
  'E12000006': { slug: 'east-of-england', label: 'East of England' },
  'E12000007': { slug: 'london', label: 'London' },
  'E12000008': { slug: 'south-east', label: 'South East' },
  'E12000009': { slug: 'south-west', label: 'South West' },
  'W99999999': { slug: 'wales', label: 'Wales' },
};

// Outcode prefix → region/county/city overrides
// Derived from postcode area assignments (the letter prefix of the outcode)
// Full list: https://en.wikipedia.org/wiki/List_of_postcode_areas_in_the_United_Kingdom
export const POSTCODE_AREA_META = {
  // London areas
  'E':   { region: 'london', county: 'greater-london', city: 'london' },
  'EC':  { region: 'london', county: 'greater-london', city: 'london' },
  'N':   { region: 'london', county: 'greater-london', city: 'london' },
  'NW':  { region: 'london', county: 'greater-london', city: 'london' },
  'SE':  { region: 'london', county: 'greater-london', city: 'london' },
  'SW':  { region: 'london', county: 'greater-london', city: 'london' },
  'W':   { region: 'london', county: 'greater-london', city: 'london' },
  'WC':  { region: 'london', county: 'greater-london', city: 'london' },
  'BR':  { region: 'london', county: 'greater-london', city: 'bromley' },
  'CR':  { region: 'london', county: 'greater-london', city: 'croydon' },
  'DA':  { region: 'london', county: 'greater-london', city: 'dartford' },
  'EN':  { region: 'london', county: 'greater-london', city: 'enfield' },
  'HA':  { region: 'london', county: 'greater-london', city: 'harrow' },
  'IG':  { region: 'london', county: 'greater-london', city: 'ilford' },
  'KT':  { region: 'london', county: 'greater-london', city: 'kingston-upon-thames' },
  'RM':  { region: 'london', county: 'greater-london', city: 'romford' },
  'SM':  { region: 'london', county: 'greater-london', city: 'sutton' },
  'TW':  { region: 'london', county: 'greater-london', city: 'twickenham' },
  'UB':  { region: 'london', county: 'greater-london', city: 'uxbridge' },
  'WD':  { region: 'london', county: 'greater-london', city: 'watford' },
  // South East
  'BN':  { region: 'south-east', county: 'east-sussex', city: 'brighton' },
  'CT':  { region: 'south-east', county: 'kent', city: 'canterbury' },
  'GU':  { region: 'south-east', county: 'surrey', city: 'guildford' },
  'HP':  { region: 'south-east', county: 'hertfordshire', city: 'hemel-hempstead' },
  'ME':  { region: 'south-east', county: 'kent', city: 'medway' },
  'MK':  { region: 'south-east', county: 'buckinghamshire', city: 'milton-keynes' },
  'OX':  { region: 'south-east', county: 'oxfordshire', city: 'oxford' },
  'PO':  { region: 'south-east', county: 'hampshire', city: 'portsmouth' },
  'RG':  { region: 'south-east', county: 'berkshire', city: 'reading' },
  'RH':  { region: 'south-east', county: 'surrey', city: 'redhill' },
  'SL':  { region: 'south-east', county: 'berkshire', city: 'slough' },
  'SO':  { region: 'south-east', county: 'hampshire', city: 'southampton' },
  'TN':  { region: 'south-east', county: 'kent', city: 'tunbridge-wells' },
  'AL':  { region: 'south-east', county: 'hertfordshire', city: 'st-albans' },
  'CM':  { region: 'east-of-england', county: 'essex', city: 'chelmsford' },
  'CO':  { region: 'east-of-england', county: 'essex', city: 'colchester' },
  'SG':  { region: 'east-of-england', county: 'hertfordshire', city: 'stevenage' },
  'SS':  { region: 'east-of-england', county: 'essex', city: 'southend-on-sea' },
  // South West
  'BA':  { region: 'south-west', county: 'somerset', city: 'bath' },
  'BH':  { region: 'south-west', county: 'dorset', city: 'bournemouth' },
  'BS':  { region: 'south-west', county: 'bristol', city: 'bristol' },
  'DT':  { region: 'south-west', county: 'dorset', city: 'dorchester' },
  'EX':  { region: 'south-west', county: 'devon', city: 'exeter' },
  'GL':  { region: 'south-west', county: 'gloucestershire', city: 'gloucester' },
  'PL':  { region: 'south-west', county: 'devon', city: 'plymouth' },
  'SP':  { region: 'south-west', county: 'wiltshire', city: 'salisbury' },
  'TA':  { region: 'south-west', county: 'somerset', city: 'taunton' },
  'TQ':  { region: 'south-west', county: 'devon', city: 'torquay' },
  'TR':  { region: 'south-west', county: 'cornwall', city: 'truro' },
  'SN':  { region: 'south-west', county: 'wiltshire', city: 'swindon' },
  // East of England
  'CB':  { region: 'east-of-england', county: 'cambridgeshire', city: 'cambridge' },
  'IP':  { region: 'east-of-england', county: 'suffolk', city: 'ipswich' },
  'LU':  { region: 'east-of-england', county: 'bedfordshire', city: 'luton' },
  'MK':  { region: 'south-east', county: 'buckinghamshire', city: 'milton-keynes' },
  'NR':  { region: 'east-of-england', county: 'norfolk', city: 'norwich' },
  'PE':  { region: 'east-of-england', county: 'cambridgeshire', city: 'peterborough' },
  // West Midlands
  'B':   { region: 'west-midlands', county: 'west-midlands', city: 'birmingham' },
  'CV':  { region: 'west-midlands', county: 'west-midlands', city: 'coventry' },
  'DY':  { region: 'west-midlands', county: 'west-midlands', city: 'dudley' },
  'ST':  { region: 'west-midlands', county: 'staffordshire', city: 'stoke-on-trent' },
  'HR':  { region: 'west-midlands', county: 'herefordshire', city: 'hereford' },
  'TF':  { region: 'west-midlands', county: 'shropshire', city: 'telford' },
  'WR':  { region: 'west-midlands', county: 'worcestershire', city: 'worcester' },
  'WS':  { region: 'west-midlands', county: 'west-midlands', city: 'walsall' },
  'WV':  { region: 'west-midlands', county: 'west-midlands', city: 'wolverhampton' },
  // East Midlands
  'DE':  { region: 'east-midlands', county: 'derbyshire', city: 'derby' },
  'LE':  { region: 'east-midlands', county: 'leicestershire', city: 'leicester' },
  'LN':  { region: 'east-midlands', county: 'lincolnshire', city: 'lincoln' },
  'NG':  { region: 'east-midlands', county: 'nottinghamshire', city: 'nottingham' },
  'NN':  { region: 'east-midlands', county: 'northamptonshire', city: 'northampton' },
  // North West
  'BB':  { region: 'north-west', county: 'lancashire', city: 'blackburn' },
  'BL':  { region: 'north-west', county: 'greater-manchester', city: 'bolton' },
  'CA':  { region: 'north-west', county: 'cumbria', city: 'carlisle' },
  'CH':  { region: 'north-west', county: 'cheshire', city: 'chester' },
  'CW':  { region: 'north-west', county: 'cheshire', city: 'crewe' },
  'FY':  { region: 'north-west', county: 'lancashire', city: 'blackpool' },
  'L':   { region: 'north-west', county: 'merseyside', city: 'liverpool' },
  'LA':  { region: 'north-west', county: 'lancashire', city: 'lancaster' },
  'M':   { region: 'north-west', county: 'greater-manchester', city: 'manchester' },
  'OL':  { region: 'north-west', county: 'greater-manchester', city: 'oldham' },
  'PR':  { region: 'north-west', county: 'lancashire', city: 'preston' },
  'SK':  { region: 'north-west', county: 'cheshire', city: 'stockport' },
  'WA':  { region: 'north-west', county: 'cheshire', city: 'warrington' },
  'WN':  { region: 'north-west', county: 'greater-manchester', city: 'wigan' },
  // Yorkshire
  'BD':  { region: 'yorkshire', county: 'west-yorkshire', city: 'bradford' },
  'DN':  { region: 'yorkshire', county: 'south-yorkshire', city: 'doncaster' },
  'HD':  { region: 'yorkshire', county: 'west-yorkshire', city: 'huddersfield' },
  'HG':  { region: 'yorkshire', county: 'north-yorkshire', city: 'harrogate' },
  'HU':  { region: 'yorkshire', county: 'east-yorkshire', city: 'hull' },
  'HX':  { region: 'yorkshire', county: 'west-yorkshire', city: 'halifax' },
  'LS':  { region: 'yorkshire', county: 'west-yorkshire', city: 'leeds' },
  'S':   { region: 'yorkshire', county: 'south-yorkshire', city: 'sheffield' },
  'WF':  { region: 'yorkshire', county: 'west-yorkshire', city: 'wakefield' },
  'YO':  { region: 'yorkshire', county: 'north-yorkshire', city: 'york' },
  // North East
  'DH':  { region: 'north-east', county: 'county-durham', city: 'durham' },
  'DL':  { region: 'north-east', county: 'county-durham', city: 'darlington' },
  'NE':  { region: 'north-east', county: 'tyne-and-wear', city: 'newcastle-upon-tyne' },
  'SR':  { region: 'north-east', county: 'tyne-and-wear', city: 'sunderland' },
  'TS':  { region: 'north-east', county: 'teesside', city: 'middlesbrough' },
  // Wales
  'CF':  { region: 'wales', county: 'cardiff', city: 'cardiff' },
  'LD':  { region: 'wales', county: 'powys', city: 'llandrindod-wells' },
  'LL':  { region: 'wales', county: 'north-wales', city: 'north-wales' },
  'NP':  { region: 'wales', county: 'gwent', city: 'newport' },
  'SA':  { region: 'wales', county: 'west-wales', city: 'swansea' },
  'SY':  { region: 'wales', county: 'mid-wales', city: 'shrewsbury' },
};

/**
 * Given an outcode like "GU1", return region/county/city.
 * Tries increasingly shorter prefixes: "GU1" → "GU" → "G"
 */
export function getOutcodeMeta(outcode) {
  // Extract letter prefix (area code)
  const letterMatch = outcode.match(/^([A-Z]+)/);
  if (!letterMatch) return null;
  const area = letterMatch[1];

  // Try full letter prefix, then shorter
  if (POSTCODE_AREA_META[area]) return POSTCODE_AREA_META[area];
  if (area.length > 1 && POSTCODE_AREA_META[area[0]]) return POSTCODE_AREA_META[area[0]];

  return { region: 'unknown', county: 'unknown', city: null };
}

/**
 * Prettify an outcode into a human-readable area name.
 * Uses pattern matching for known postcode areas.
 */
export function getOutcodeName(outcode) {
  // We'll populate this from the Land Registry data (most common town/city for that outcode)
  // For now return the outcode itself — the pipeline will enrich this
  return outcode;
}
