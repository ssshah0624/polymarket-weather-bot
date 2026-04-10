# CPI Strategy Design

## Scope

This is the next rules-based strategy after weather. Version 1 is limited to:

- Headline CPI
- Core CPI
- Kalshi first, with venue-agnostic modeling where possible

The goal is the same as weather: build a transparent paper-trading loop where every decision can be explained, reviewed, and improved without automatic self-modification.

## Release Cadence

- CPI is released monthly by the U.S. Bureau of Labor Statistics.
- Settlement should use the official BLS CPI release for the exact series and horizon the contract references.
- The bot should only trade the next scheduled CPI release, never multiple releases at once.

## Primary Data Sources

- BLS release calendar for scheduled release timing
- BLS CPI release tables for official settlement values
- Cleveland Fed Inflation Nowcasting for the primary pre-release forecast
- FRED and BLS historical CPI series for backfill and model-error estimation

## Core Modeling Rules

### 1. Forecast Input

- `model_mean` = latest available Cleveland Fed nowcast for the exact metric being traded
- Supported metrics:
  - Headline CPI MoM
  - Headline CPI YoY
  - Core CPI MoM
  - Core CPI YoY

### 2. Error Model

- `model_sigma` = rolling RMSE of the Cleveland Fed nowcast versus the realized BLS print for the last `24` releases of that exact metric
- Sigma floors:
  - `0.05` for MoM contracts
  - `0.10` for YoY contracts

### 3. Probability Conversion

- Convert the forecast mean and sigma into a continuous distribution
- Integrate that distribution across each Kalshi contract bin
- Produce a probability for every contract bin in the market strip
- Compare model probability to venue entry price after the configured fee/risk buffer

## Paper-Trading Rules

- Trade only the next scheduled CPI release
- Open positions from `T-7 days` to `T-30 minutes`
- Stop opening new positions inside `30 minutes` of the release
- Use the same capped Kelly sizing framework as weather
- Deduplicate by venue, release date, and contract bin

## Required Decision Trace

Every paper trade should store:

- strategy version
- release date and metric
- source nowcast value
- nowcast timestamp
- rolling RMSE used as sigma
- venue market strip at decision time
- chosen contract
- selected side
- entry price
- model probability
- edge after fees
- explicit skip reason if no trade was placed

## Learning Loop

The CPI learning loop should mirror weather but remain recommendation-only.

Weekly review should analyze:

- edge bucket performance
- Kalshi performance by metric type
- timing performance by entry window
- nowcast bias versus realized CPI
- sigma calibration quality

Recommendations may adjust:

- minimum tradable edge
- fee/risk buffer
- entry window
- sigma floor

Recommendations must remain human-reviewed until there is enough evidence to justify bounded shadow tuning.

## Why CPI Fits The Framework

- Objective settlement source
- Scheduled release times
- Explicit pre-release hypothesis
- Market bins that map cleanly to probabilities
- Clean distinction between model forecast, market view, and realized outcome
