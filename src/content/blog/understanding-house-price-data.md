---
title: "Understanding House Price Data: How Land Registry Figures Actually Work"
description: "A plain-English guide to HM Land Registry Price Paid Data — what it includes, what it misses, and how to interpret average house prices without being misled."
pubDate: "2026-03-20"
category: "Data Explained"
readTime: 7
---

When you see a headline like "Average UK house price hits £287,000", it sounds simple. One number, one country. In practice, that figure is the product of a specific methodology, a specific data source, and a set of decisions about what to include and exclude. Understanding those decisions is the difference between using house price data well and being misled by it.

## Where House Price Data Comes From

There are three main sources of UK house price data, and they do not always agree:

| Source | What It Measures | Update Frequency |
|--------|-----------------|-----------------|
| **HM Land Registry** | Actual completed sales (price paid) | Monthly, ~2 month lag |
| **ONS House Price Index** | Repeat sales with mix adjustment | Monthly |
| **Halifax / Nationwide** | Mortgage approval values at their bank | Monthly |

We use Land Registry Price Paid Data on Postcode.Page because it covers every residential property sale in England and Wales — not just mortgage-funded purchases at one particular bank.

## What Price Paid Data Includes

Land Registry records every completed residential property transaction, including:

- **Freehold and leasehold sales** (though the mix varies by area — London is heavily leasehold)
- **New-build and existing properties** (flagged separately, which matters because new-builds carry a premium)
- **Full market value transactions** (transfers between family members at below market value are excluded)

Each record includes the price, the date, the property type (detached, semi-detached, terraced, flat), whether it is new-build, and the full address including postcode.

## What It Does Not Include

Price Paid Data has significant blind spots:

**No rental data.** It only covers sales. In areas with high rental populations, the data reflects the buying market, which may be very different from the overall housing market.

**No Right to Buy sales below market value.** These are excluded, which can understate activity in areas with large council housing stock.

**No commercial property.** Mixed-use properties (a flat above a shop, for example) may be excluded or recorded differently.

**Two-month reporting lag.** A sale that completes in January typically appears in the data in March. This means published averages always trail the actual market by at least two months.

## How Averages Can Mislead

The "average house price" for a postcode district is a mean — the sum of all sale prices divided by the number of sales. This is sensitive to outliers.

Consider a postcode district with ten sales:

- Nine terraced houses at £180,000 each: total £1,620,000
- One detached house at £750,000

The mean is £237,000. But the *typical* buyer in this area paid £180,000. The mean overstates the everyday experience by 32%.

This is why we show **price by property type** on every postcode page. The average for terraced houses, semis, detached, and flats tells a much more useful story than a single district-wide number.

### Median vs Mean

The median (the middle value when all sales are ranked by price) is more resistant to outliers. For areas with a wide range of property types, the median is often a better guide to "what you would actually pay".

Land Registry publishes both mean and median figures. We display the mean on Postcode.Page because it is more widely recognised, but we contextualise it with type breakdowns and comparisons.

## Year-on-Year Change: What It Actually Means

When we show "+3.2% year-on-year" for a postcode district, we are comparing the average price in the most recent 12 months with the average from the preceding 12 months.

This is a *lagging indicator*. It tells you what happened, not what is happening now. More importantly, it can be distorted by:

**Mix shift.** If more expensive property types sold this year than last, the average rises even if no individual property increased in value.

**Volume changes.** A district with 200 sales per year produces a more reliable average than one with 15. Small districts can show wild percentage swings that reflect statistical noise rather than genuine market movement.

**Seasonal patterns.** The UK housing market has consistent seasonal patterns. Spring and autumn see higher volumes and sometimes higher prices. Comparing any single month against the same month a year earlier can be more useful than comparing rolling 12-month windows.

## How to Read a Postcode.Page Price Section

When you visit a postcode page on this site, here is what each figure means:

**Average House Price** — Mean of all residential sales in the district over the past 12 months. Gives you a general price level.

**1yr Change** — Percentage change in the mean compared to the preceding 12 months. Indicates direction of travel.

**Price by Property Type** — Separate averages for detached, semi-detached, terraced, and flats. This is usually more useful than the headline average.

**vs National Average** — Percentage above or below the England & Wales mean. Helps you calibrate whether an area is expensive or affordable in a national context.

**vs County Average** — Percentage above or below the county-level mean. More useful for local comparisons — being 10% above the county average matters more than being 50% below the London average.

## Using Price Data for Buying Decisions

If you are researching an area to buy in, here is how to use house price data effectively:

### Do This

- **Compare like with like.** Use the property type breakdown, not the headline average. If you are buying a terrace, compare terrace prices.
- **Look at the trend over 3-5 years**, not just 1 year. A single year can be an anomaly. A consistent trend is a signal.
- **Cross-reference with volume.** If the number of sales has dropped sharply, the average may be unreliable. It could also signal a stagnating market.
- **Check nearby districts.** If one postcode is significantly cheaper than its neighbours, find out why. It could be a bargain — or there could be a reason.

### Avoid This

- **Do not treat the average as "what you will pay".** The average includes all property types and all conditions. A renovated semi in a good street will cost more than the district average for semis.
- **Do not assume rising prices are good news if you are buying.** Rising prices benefit sellers and existing owners. As a buyer, you want stability or gentle decline in the short term.
- **Do not compare across regions without context.** £300,000 in central Manchester buys a very different life than £300,000 in rural Devon. Use affordability ratios (price vs local earnings) for cross-region comparison.

## Why We Chose This Data Source

We considered Zoopla estimates, Rightmove asking prices, and mortgage lender indices before settling on Land Registry as our primary source. The decision came down to three factors:

1. **Coverage.** Land Registry records every sale, not just those funded by one bank.
2. **Accuracy.** These are actual completion prices, not asking prices or estimated values.
3. **Transparency.** The data is published under the Open Government Licence and can be independently verified.

The trade-off is the reporting lag. By the time a sale appears in our data, it is two to three months old. For a monthly snapshot of market direction, mortgage lender indices are faster. For understanding what people actually paid for property in a specific area, Land Registry is the authoritative source.

---

*Search any postcode district on [Postcode.Page](/) to see the latest house price data, broken down by property type, with comparisons to county and national averages.*
