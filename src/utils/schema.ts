export interface FAQItem {
  question: string;
  answer: string;
}

export function buildFAQSchema(faqs: FAQItem[], url: string) {
  return {
    "@type": "FAQPage",
    "@id": `${url}#faq`,
    "mainEntity": faqs.map(f => ({
      "@type": "Question",
      "name": f.question,
      "acceptedAnswer": {
        "@type": "Answer",
        "text": f.answer,
      },
    })),
  };
}

interface PostcodeData {
  code: string;
  name: string;
  county: string;
  region: string;
  coordinates?: { lat: number; lng: number };
  meta: { lastUpdated: string };
}

export function buildPostcodeSchema(data: PostcodeData, url: string) {
  const regionLabel = data.region.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  const countyLabel = data.county.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  return [
    {
      "@type": "WebPage",
      "@id": `${url}#webpage`,
      "url": url,
      "name": `${data.code} — ${data.name} | Postcode.Page`,
      "dateModified": data.meta.lastUpdated,
      "breadcrumb": { "@id": `${url}#breadcrumb` },
      "mainEntity": { "@id": `${url}#place` }
    },
    {
      "@type": "Place",
      "@id": `${url}#place`,
      "name": `${data.code} — ${data.name}`,
      "address": {
        "@type": "PostalAddress",
        "postalCode": data.code,
        "addressRegion": countyLabel,
        "addressCountry": "GB"
      },
      ...(data.coordinates ? {
        "geo": {
          "@type": "GeoCoordinates",
          "latitude": data.coordinates.lat,
          "longitude": data.coordinates.lng
        }
      } : {}),
      "containedInPlace": {
        "@type": "AdministrativeArea",
        "name": countyLabel,
        "url": `https://postcode.page/counties/${data.county}/`
      }
    },
    {
      "@type": "Dataset",
      "name": `${data.code} Postcode Area Data`,
      "description": `Property prices, crime statistics, school ratings, demographics, and broadband speeds for ${data.code} (${data.name})`,
      "url": url,
      "creator": {
        "@type": "Organization",
        "name": "Postcode.Page",
        "url": "https://postcode.page/"
      },
      "license": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
      "isBasedOn": [
        "https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads",
        "https://data.police.uk/",
        "https://get-information-schools.service.gov.uk/",
        "https://www.ons.gov.uk/census"
      ]
    },
    {
      "@type": "BreadcrumbList",
      "@id": `${url}#breadcrumb`,
      "itemListElement": [
        { "@type": "ListItem", "position": 1, "name": "Home", "item": "https://postcode.page/" },
        { "@type": "ListItem", "position": 2, "name": regionLabel, "item": `https://postcode.page/regions/${data.region}/` },
        { "@type": "ListItem", "position": 3, "name": countyLabel, "item": `https://postcode.page/counties/${data.county}/` },
        { "@type": "ListItem", "position": 4, "name": data.code }
      ]
    }
  ];
}
