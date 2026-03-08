export function formatPrice(n: number): string {
  if (n >= 1_000_000) return `£${(n / 1_000_000).toFixed(2)}m`;
  if (n >= 1_000) return `£${Math.round(n / 1_000).toLocaleString()}K`;
  return `£${n.toLocaleString()}`;
}

export function formatPriceFull(n: number): string {
  return `£${Math.round(n).toLocaleString()}`;
}

export function formatPct(n: number, decimals = 1): string {
  return `${n.toFixed(decimals)}%`;
}

export function formatChange(n: number): string {
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(1)}%`;
}

export function formatSpeed(n: number): string {
  return `${n.toFixed(1)} Mbps`;
}

export function pctDiff(a: number, b: number): number {
  return ((a - b) / b) * 100;
}

export function capitalize(s: string): string {
  return s.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

export function slugToCode(slug: string): string {
  return slug.toUpperCase();
}

export function codeToSlug(code: string): string {
  return code.toLowerCase().replace(/\s+/g, '');
}

export function crimeSentiment(vsNational: number): 'positive' | 'negative' | 'neutral' {
  if (vsNational < -10) return 'positive';
  if (vsNational > 10) return 'negative';
  return 'neutral';
}

export function priceSentiment(vsNational: number): 'positive' | 'negative' | 'neutral' {
  // For buyers, below national is positive; above is negative
  if (vsNational < -10) return 'positive';
  if (vsNational > 10) return 'negative';
  return 'neutral';
}

export function schoolSentiment(outstanding: number, total: number): 'positive' | 'negative' | 'neutral' {
  if (total === 0) return 'neutral';
  const ratio = outstanding / total;
  if (ratio > 0.25) return 'positive';
  if (ratio < 0.1) return 'negative';
  return 'neutral';
}
