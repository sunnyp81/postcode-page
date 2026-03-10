---
title: "What Land Registry Price Paid Data Tells You (and What It Doesn't)"
description: "HM Land Registry Price Paid Data is the most reliable source of UK house price information — but understanding its limitations helps you use it more accurately."
pubDate: "2026-03-09"
category: "House Prices"
readTime: 6
---

HM Land Registry Price Paid Data (PPD) is the foundation of every credible UK house price index, and it is the source that Postcode.Page uses for all district and sector price data. It is free, comprehensive, and goes back to 1995. But like all data, it has constraints worth understanding.

## What It Is

The Land Registry records every residential property sale in England and Wales once the transaction is registered. The data includes:

- **Sale price** (the agreed purchase price)
- **Date of transfer** (registration date, not exchange date — typically 2–6 weeks later)
- **Full address and postcode**
- **Property type**: Detached, semi-detached, terraced, or flat/maisonette
- **Whether the property is new or old** (new build vs established)
- **Tenure**: Freehold or leasehold
- **Transaction category**: Standard (A) or additional (B — covering buy-to-let and company purchases)

## Why It Is Reliable

Unlike asking price data from Rightmove or Zoopla, PPD records **actual sale prices** — what buyers legally committed to pay and what appeared on the transfer deed. There is no incentive to inflate or deflate these figures; they are official legal records.

Coverage is near-total for England and Wales. The only gaps are:

- Transactions that were not registered (rare, typically older or unusual arrangements)
- Scotland and Northern Ireland (separate land registries, different data)
- Properties sold below market value (staircasing on shared ownership, family transfers)

## What It Does Not Capture

**The mix problem**: An area's average price can change not because values have risen but because the *mix* of properties sold has changed. If a district sells mostly flats one year and mostly detached houses the next, the average will change even if individual property values have not. The official HM Land Registry House Price Index attempts to correct for this using a repeat-sales regression model — but the raw PPD data does not.

Postcode.page uses raw PPD averages, which is standard practice for district-level comparisons (the mix effect averages out over time), but you should be aware that single-year changes in smaller districts can be mix-driven rather than value-driven.

**New build premium**: New build properties typically sell at a 20–30% premium to equivalent second-hand properties. Districts with high new build activity will show inflated averages. Postcode.page does not currently separate new builds from established properties in the headline average, though the data is available in the underlying records.

**Registration lag**: The Land Registry data reflects the date of *registration*, which lags the date of *exchange* by weeks or months. In a fast-moving market, PPD averages can trail real-time market conditions by 3–6 months.

**Off-market and corporate transactions**: Bulk purchases (developer buying a block, corporate acquisitions) appear in the data. PPD category B transactions are flagged and should ideally be excluded from residential market averages — this is handled in the Land Registry's official index but may not be in all third-party uses of the data.

## How Postcode.page Uses the Data

- **12-month window** for current average prices: reduces seasonality and mix effects
- **Minimum transaction thresholds**: Districts with very few sales are flagged (the average is less statistically robust for small markets)
- **Property type breakdown**: Detached, semi, terraced, and flat averages are calculated separately so you can see the full distribution
- **5-year history**: Annual averages from 2020–2025 let you assess the medium-term trend

## What to Watch For

**Small districts**: A district with only 30 sales in 12 months can have its average distorted by a single atypical transaction. The fewer the transactions, the less reliable the average.

**Year-on-year swings**: A district that appears to have risen 15% in one year may simply have sold more expensive properties that year. Check whether the mix (proportion of detached vs flats) has changed before concluding prices have surged.

**Commercial postcodes**: Some postcode districts (particularly in central London — EC, WC) have significant commercial activity. Land Registry records include some non-standard residential transactions in these areas. Postcode.page filters outlier prices but some commercial contamination can remain.

---

For a full explanation of how Postcode.Page processes Land Registry data, see our [methodology page](/methodology/). To see current averages for any district, [search your postcode →](/).
