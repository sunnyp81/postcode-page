/**
 * Step 1: Download Land Registry Price Paid data via PPI JSON API.
 * Queries by admin district, sampling up to PAGES_PER_DISTRICT pages each.
 *
 * Run: node pipeline/01-download.mjs
 * Output: pipeline/raw/lr-combined.csv
 *
 * API: https://landregistry.data.gov.uk/data/ppi/transaction-record.json
 *      ?propertyAddress.district=GUILDFORD&_pageSize=100&_page=N&_sort=-transactionDate
 */

import { existsSync, mkdirSync, statSync, writeFileSync, appendFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RAW_DIR = join(__dirname, 'raw');
mkdirSync(RAW_DIR, { recursive: true });

const DELAY_MS = 350;
const PAGES_PER_DISTRICT = 6; // 600 transactions per district max
const sleep = ms => new Promise(r => setTimeout(r, ms));

const LR_API = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json';
const LR_OUT = join(RAW_DIR, 'lr-combined.csv');

// UK local authority districts as they appear in LR Price Paid data (uppercase)
const DISTRICTS = [
  // === LONDON BOROUGHS ===
  'BARKING AND DAGENHAM', 'BARNET', 'BEXLEY', 'BRENT', 'BROMLEY',
  'CAMDEN', 'CITY OF LONDON', 'CROYDON', 'EALING', 'ENFIELD',
  'GREENWICH', 'HACKNEY', 'HAMMERSMITH AND FULHAM', 'HARINGEY', 'HARROW',
  'HAVERING', 'HILLINGDON', 'HOUNSLOW', 'ISLINGTON', 'KENSINGTON AND CHELSEA',
  'KINGSTON UPON THAMES', 'LAMBETH', 'LEWISHAM', 'MERTON', 'NEWHAM',
  'REDBRIDGE', 'RICHMOND UPON THAMES', 'SOUTHWARK', 'SUTTON', 'TOWER HAMLETS',
  'WALTHAM FOREST', 'WANDSWORTH', 'WESTMINSTER',

  // === SOUTH EAST ===
  // Berkshire (unitary)
  'BRACKNELL FOREST', 'READING', 'SLOUGH', 'WINDSOR AND MAIDENHEAD', 'WOKINGHAM',
  // Hampshire
  'BASINGSTOKE AND DEANE', 'EASTLEIGH', 'FAREHAM', 'GOSPORT', 'HART',
  'HAVANT', 'NEW FOREST', 'RUSHMOOR', 'TEST VALLEY', 'WINCHESTER',
  // East Sussex / Brighton
  'BRIGHTON AND HOVE', 'EASTBOURNE', 'HASTINGS', 'LEWES', 'ROTHER', 'WEALDEN',
  // Kent
  'ASHFORD', 'CANTERBURY', 'DARTFORD', 'DOVER', 'FOLKESTONE AND HYTHE',
  'GRAVESHAM', 'MAIDSTONE', 'MEDWAY', 'SEVENOAKS', 'SHEPWAY',
  'SWALE', 'THANET', 'TONBRIDGE AND MALLING', 'TUNBRIDGE WELLS',
  // Surrey
  'ELMBRIDGE', 'EPSOM AND EWELL', 'GUILDFORD', 'MOLE VALLEY', 'REIGATE AND BANSTEAD',
  'RUNNYMEDE', 'SPELTHORNE', 'SURREY HEATH', 'TANDRIDGE', 'WAVERLEY', 'WOKING',
  // West Sussex
  'ADUR', 'ARUN', 'CHICHESTER', 'CRAWLEY', 'HORSHAM', 'MID SUSSEX', 'WORTHING',
  // Oxfordshire
  'CHERWELL', 'OXFORD', 'SOUTH OXFORDSHIRE', 'VALE OF WHITE HORSE', 'WEST OXFORDSHIRE',
  // Buckinghamshire
  'BUCKINGHAMSHIRE', 'AYLESBURY VALE', 'CHILTERN', 'SOUTH BUCKS', 'WYCOMBE',
  // Isle of Wight / Milton Keynes
  'ISLE OF WIGHT', 'MILTON KEYNES',

  // === HERTFORDSHIRE ===
  'BROXBOURNE', 'DACORUM', 'EAST HERTFORDSHIRE', 'HERTSMERE', 'NORTH HERTFORDSHIRE',
  'ST ALBANS', 'STEVENAGE', 'THREE RIVERS', 'WATFORD', 'WELWYN HATFIELD',

  // === EAST OF ENGLAND ===
  // Cambridgeshire
  'CAMBRIDGE', 'EAST CAMBRIDGESHIRE', 'FENLAND', 'HUNTINGDONSHIRE',
  'PETERBOROUGH', 'SOUTH CAMBRIDGESHIRE',
  // Essex
  'BASILDON', 'BRAINTREE', 'BRENTWOOD', 'CASTLE POINT', 'CHELMSFORD',
  'COLCHESTER', 'EPPING FOREST', 'HARLOW', 'MALDON', 'ROCHFORD',
  'SOUTHEND-ON-SEA', 'TENDRING', 'THURROCK', 'UTTLESFORD',
  // Norfolk
  'BRECKLAND', 'BROADLAND', 'GREAT YARMOUTH', "KING'S LYNN AND WEST NORFOLK",
  'NORTH NORFOLK', 'NORWICH', 'SOUTH NORFOLK',
  // Suffolk (old and new district names)
  'BABERGH', 'EAST SUFFOLK', 'IPSWICH', 'MID SUFFOLK', 'WEST SUFFOLK',
  'FOREST HEATH', 'ST EDMUNDSBURY', 'SUFFOLK COASTAL', 'WAVENEY',
  // Bedfordshire
  'BEDFORD', 'CENTRAL BEDFORDSHIRE', 'LUTON',

  // === EAST MIDLANDS ===
  // Derbyshire
  'AMBER VALLEY', 'BOLSOVER', 'CHESTERFIELD', 'DERBY', 'DERBYSHIRE DALES',
  'EREWASH', 'HIGH PEAK', 'NORTH EAST DERBYSHIRE', 'SOUTH DERBYSHIRE',
  // Leicestershire
  'BLABY', 'CHARNWOOD', 'HARBOROUGH', 'HINCKLEY AND BOSWORTH', 'LEICESTER',
  'MELTON', 'NORTH WEST LEICESTERSHIRE', 'OADBY AND WIGSTON',
  // Lincolnshire
  'BOSTON', 'EAST LINDSEY', 'LINCOLN', 'NORTH KESTEVEN', 'RUTLAND',
  'SOUTH HOLLAND', 'SOUTH KESTEVEN', 'WEST LINDSEY',
  // Northamptonshire (old and new)
  'CORBY', 'DAVENTRY', 'EAST NORTHAMPTONSHIRE', 'KETTERING', 'NORTHAMPTON',
  'NORTH NORTHAMPTONSHIRE', 'SOUTH NORTHAMPTONSHIRE', 'WELLINGBOROUGH', 'WEST NORTHAMPTONSHIRE',
  // Nottinghamshire
  'ASHFIELD', 'BROXTOWE', 'GEDLING', 'MANSFIELD',
  'NEWARK AND SHERWOOD', 'NOTTINGHAM', 'RUSHCLIFFE',

  // === WEST MIDLANDS ===
  // Metropolitan
  'BIRMINGHAM', 'COVENTRY', 'DUDLEY', 'SANDWELL', 'SOLIHULL', 'WALSALL', 'WOLVERHAMPTON',
  // Staffordshire
  'CANNOCK CHASE', 'EAST STAFFORDSHIRE', 'LICHFIELD', 'NEWCASTLE-UNDER-LYME',
  'SOUTH STAFFORDSHIRE', 'STAFFORD', 'STAFFORDSHIRE MOORLANDS', 'STOKE-ON-TRENT', 'TAMWORTH',
  // Worcestershire
  'BROMSGROVE', 'MALVERN HILLS', 'REDDITCH', 'WORCESTER', 'WYCHAVON', 'WYRE FOREST',
  // Herefordshire / Shropshire
  'HEREFORDSHIRE', 'SHROPSHIRE', 'TELFORD AND WREKIN',
  // Warwickshire
  'NORTH WARWICKSHIRE', 'NUNEATON AND BEDWORTH', 'RUGBY', 'STRATFORD-ON-AVON', 'WARWICK',

  // === YORKSHIRE AND THE HUMBER ===
  // Metropolitan
  'BARNSLEY', 'BRADFORD', 'CALDERDALE', 'DONCASTER', 'KIRKLEES',
  'LEEDS', 'ROTHERHAM', 'SHEFFIELD', 'WAKEFIELD',
  // North Yorkshire
  'CRAVEN', 'HAMBLETON', 'HARROGATE', 'RICHMONDSHIRE', 'RYEDALE',
  'SCARBOROUGH', 'SELBY', 'YORK', 'NORTH YORKSHIRE',
  // East / Humber
  'EAST RIDING OF YORKSHIRE', 'KINGSTON UPON HULL',
  'NORTH EAST LINCOLNSHIRE', 'NORTH LINCOLNSHIRE',

  // === NORTH WEST ===
  // Cheshire (old and new)
  'CHESHIRE EAST', 'CHESHIRE WEST AND CHESTER', 'HALTON', 'WARRINGTON',
  'CHESTER', 'ELLESMERE PORT AND NESTON', 'VALE ROYAL', 'MACCLESFIELD',
  'CREWE AND NANTWICH', 'CONGLETON',
  // Cumbria (old and new)
  'ALLERDALE', 'BARROW-IN-FURNESS', 'CARLISLE', 'COPELAND', 'EDEN', 'SOUTH LAKELAND',
  'WESTMORLAND AND FURNESS', 'CUMBERLAND',
  // Lancashire
  'BURNLEY', 'CHORLEY', 'FYLDE', 'HYNDBURN', 'LANCASTER', 'PENDLE',
  'PRESTON', 'RIBBLE VALLEY', 'ROSSENDALE', 'SOUTH RIBBLE', 'WEST LANCASHIRE', 'WYRE',
  'BLACKBURN WITH DARWEN', 'BLACKPOOL',
  // Greater Manchester
  'BOLTON', 'BURY', 'MANCHESTER', 'OLDHAM', 'ROCHDALE', 'SALFORD',
  'STOCKPORT', 'TAMESIDE', 'TRAFFORD', 'WIGAN',
  // Merseyside
  'KNOWSLEY', 'LIVERPOOL', 'SEFTON', 'ST HELENS', 'WIRRAL',

  // === NORTH EAST ===
  'COUNTY DURHAM', 'DARLINGTON', 'GATESHEAD', 'HARTLEPOOL', 'MIDDLESBROUGH',
  'NEWCASTLE UPON TYNE', 'NORTH TYNESIDE', 'NORTHUMBERLAND',
  'REDCAR AND CLEVELAND', 'SOUTH TYNESIDE', 'STOCKTON-ON-TEES', 'SUNDERLAND',

  // === SOUTH WEST ===
  // Bristol area
  'BATH AND NORTH EAST SOMERSET', 'BRISTOL', 'NORTH SOMERSET', 'SOUTH GLOUCESTERSHIRE',
  // Gloucestershire
  'CHELTENHAM', 'COTSWOLD', 'FOREST OF DEAN', 'GLOUCESTER', 'STROUD', 'TEWKESBURY',
  // Devon
  'EAST DEVON', 'EXETER', 'MID DEVON', 'NORTH DEVON', 'PLYMOUTH',
  'SOUTH HAMS', 'TEIGNBRIDGE', 'TORBAY', 'TORRIDGE', 'WEST DEVON',
  // Cornwall
  'CORNWALL',
  // Somerset (old and new)
  'MENDIP', 'SEDGEMOOR', 'SOMERSET WEST AND TAUNTON', 'SOUTH SOMERSET',
  'TAUNTON DEANE', 'WEST SOMERSET', 'SOMERSET',
  // Dorset (old and new)
  'BOURNEMOUTH', 'CHRISTCHURCH', 'POOLE', 'DORSET',
  'BOURNEMOUTH CHRISTCHURCH AND POOLE',
  'EAST DORSET', 'NORTH DORSET', 'PURBECK', 'WEST DORSET', 'WEYMOUTH AND PORTLAND',
  // Wiltshire
  'SWINDON', 'WILTSHIRE', 'KENNET', 'NORTH WILTSHIRE', 'SALISBURY', 'WEST WILTSHIRE',

  // === WALES ===
  'BLAENAU GWENT', 'BRIDGEND', 'CAERPHILLY', 'CARDIFF', 'CARMARTHENSHIRE',
  'CEREDIGION', 'CONWY', 'DENBIGHSHIRE', 'FLINTSHIRE', 'GWYNEDD',
  'ISLE OF ANGLESEY', 'MERTHYR TYDFIL', 'MONMOUTHSHIRE', 'NEATH PORT TALBOT',
  'NEWPORT', 'PEMBROKESHIRE', 'POWYS', 'RHONDDA CYNON TAF', 'SWANSEA',
  'TORFAEN', 'VALE OF GLAMORGAN', 'WREXHAM',
];

async function fetchJSON(url) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' },
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 429) { await sleep(5000); continue; }
      if (!res.ok) return null;
      return await res.json();
    } catch {
      if (attempt === 2) return null;
      await sleep(2000 * (attempt + 1));
    }
  }
  return null;
}

function parseItems(json) {
  // LR linked data API: items live at result.items (not result.primaryTopic.items)
  return json?.result?.primaryTopic?.items
    ?? json?.result?.items
    ?? json?.items
    ?? [];
}

// Extract the short label from LR JSON-LD objects like
// { prefLabel: [{ _value: "terraced", ... }], label: [...] }
function getLabel(obj) {
  if (obj == null) return '';
  if (typeof obj !== 'object') return String(obj);
  // prefLabel is an array of {_value, _datatype, _lang}
  const pl = obj.prefLabel;
  if (Array.isArray(pl) && pl.length) return pl[0]._value ?? '';
  const lb = obj.label;
  if (Array.isArray(lb) && lb.length) return lb[0]._value ?? '';
  // Fall back to last segment of @id / _about URL
  const id = obj['_about'] ?? obj['@id'] ?? '';
  return id.split('/').pop() ?? '';
}

// Parse LR date string: "Wed, 28 Jan 2026" → "2026-01-28"
function parseDate(raw) {
  if (!raw) return '';
  try {
    const d = new Date(raw);
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  } catch {}
  return String(raw).slice(0, 10);
}

// Map LR property type label to single letter code
const PROP_TYPE_MAP = {
  terraced: 'T', semidetached: 'S', 'semi-detached': 'S',
  detached: 'D', flat: 'F', maisonette: 'F', 'flat/maisonette': 'F',
  other: 'O',
};

// Map LR estate type to F/L/U
const ESTATE_TYPE_MAP = { freehold: 'F', leasehold: 'L' };

// Map LR transaction category label to A/B
function ppdCategory(obj) {
  const label = getLabel(obj).toLowerCase();
  if (label.includes('additional')) return 'B';
  return 'A';
}

// Map LR record status to A/C/D
function recordStatus(obj) {
  const label = getLabel(obj).toLowerCase();
  if (label.startsWith('c') || label === 'change') return 'C';
  if (label.startsWith('d') || label === 'delete') return 'D';
  return 'A';
}

function itemToCSVRow(item) {
  const addr = item.propertyAddress ?? {};

  const transId  = item.transactionId ?? item['_about'] ?? '';
  const price    = parseInt(item.pricePaid) || 0;
  const date     = parseDate(item.transactionDate);
  const postcode = String(addr.postcode ?? '').trim();
  const propTypeLabel = getLabel(item.propertyType).toLowerCase().replace(/[\/ ]/g, '');
  const propType = PROP_TYPE_MAP[propTypeLabel] ?? 'O';
  const newBuild = item.newBuild === true || item.newBuild === 'Y' || item.newBuild === 'true' ? 'Y' : 'N';
  const tenure   = ESTATE_TYPE_MAP[getLabel(item.estateType).toLowerCase()] ?? 'U';
  const paon     = addr.paon ?? '';
  const saon     = addr.saon ?? '';
  const street   = addr.street ?? '';
  const locality = addr.locality ?? '';
  const town     = addr.town ?? '';
  const district = addr.district ?? '';
  const county   = addr.county ?? '';
  const ppdCat   = ppdCategory(item.transactionCategory);
  const recStat  = recordStatus(item.recordStatus);

  if (!postcode || !price || price < 10000) return null;

  const cols = [transId, price, date, postcode, propType, newBuild, tenure,
                paon, saon, street, locality, town, district, county, ppdCat, recStat];
  return cols.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',');
}

// ─── Schools ──────────────────────────────────────────────────────────────────
console.log('\n=== Step 1: Download Data ===\n');

const schoolsPath = join(RAW_DIR, 'schools-gias.csv');
if (existsSync(schoolsPath) && statSync(schoolsPath).size > 1_000_000) {
  console.log(`✓ Schools already downloaded (${(statSync(schoolsPath).size / 1024 / 1024).toFixed(1)}MB)`);
} else {
  console.log('📥 Downloading DfE Schools (GIAS)...');
  try {
    const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const res = await fetch(
      `https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata${dateStr}.csv`,
      { headers: { 'User-Agent': 'postcode.page data pipeline (hello@postcode.page)' } }
    );
    if (res.ok) {
      writeFileSync(schoolsPath, await res.text());
      console.log(`  ✓ ${(statSync(schoolsPath).size / 1024 / 1024).toFixed(1)}MB`);
    } else {
      console.warn('  ⚠ Schools download failed — will skip school data');
    }
  } catch (e) {
    console.warn('  ⚠ Schools download error:', e.message);
  }
}

// ─── Land Registry ─────────────────────────────────────────────────────────
if (existsSync(LR_OUT) && statSync(LR_OUT).size > 10_000_000) {
  console.log(`\n✓ Land Registry already downloaded (${(statSync(LR_OUT).size / 1024 / 1024).toFixed(0)}MB)`);
  console.log('\n✅ Download complete');
  console.log('   Next: node pipeline/02-parse-land-registry.mjs\n');
  process.exit(0);
}

console.log(`\n📥 Downloading Land Registry via PPI API (by district)...`);
console.log(`   ${DISTRICTS.length} districts × up to ${PAGES_PER_DISTRICT} pages × 100 rows`);
console.log(`   Estimated: ~${Math.round(DISTRICTS.length * PAGES_PER_DISTRICT * DELAY_MS / 60000)} minutes\n`);

writeFileSync(LR_OUT, ''); // Clear/create file

let totalRows = 0;
let successDistricts = 0;
let debugLogged = false;

for (let di = 0; di < DISTRICTS.length; di++) {
  const district = DISTRICTS[di];
  let districtRows = 0;

  for (let page = 0; page < PAGES_PER_DISTRICT; page++) {
    const url = `${LR_API}?propertyAddress.district=${encodeURIComponent(district)}`
      + `&_pageSize=100&_page=${page}&_sort=-transactionDate`;

    const json = await fetchJSON(url);

    if (!json) break;

    // Log a sample row on first successful response
    if (!debugLogged) {
      const items0 = parseItems(json);
      if (items0.length) {
        const sample = itemToCSVRow(items0[0]);
        console.log(`  [Debug] First row: ${sample?.slice(0, 120) ?? 'null'}`);
      }
      debugLogged = true;
    }

    const items = parseItems(json);
    if (!Array.isArray(items) || items.length === 0) break;

    const rows = items.map(itemToCSVRow).filter(Boolean);
    if (rows.length > 0) {
      appendFileSync(LR_OUT, rows.join('\n') + '\n');
      districtRows += rows.length;
    }

    if (items.length < 100) break; // Last page for this district

    await sleep(DELAY_MS);
  }

  if (districtRows > 0) {
    totalRows += districtRows;
    successDistricts++;
  }

  // Progress every 10 districts
  if ((di + 1) % 10 === 0 || di === DISTRICTS.length - 1) {
    const pct = Math.round((di + 1) / DISTRICTS.length * 100);
    const mb = (existsSync(LR_OUT) ? statSync(LR_OUT).size / 1024 / 1024 : 0).toFixed(1);
    process.stdout.write(`\r  [${pct}%] ${di + 1}/${DISTRICTS.length} districts | ${totalRows.toLocaleString()} rows | ${mb}MB`);
  }

  await sleep(DELAY_MS);
}

const finalMB = (statSync(LR_OUT).size / 1024 / 1024).toFixed(1);
console.log(`\n\n  ✓ ${totalRows.toLocaleString()} transactions from ${successDistricts} districts (${finalMB}MB)`);
console.log('\n✅ Download complete');
console.log('   Next: node pipeline/02-parse-land-registry.mjs\n');
