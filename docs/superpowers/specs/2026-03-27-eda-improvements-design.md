# EDA System Improvements + Pipeline Flow Fix

## Problem

1. EDA reports false warnings: holidays flagged as missing dates, ATM-relative strike drift flagged as missing combos, normal options volume skew flagged as outliers
2. No automated health check after EDA to confirm DhanHQ API + Supabase are working
3. EDA runs as a post-pipeline step so crashes in earlier stages prevent it from running

## Design

### 1. Holiday-Aware Completeness

Add `NSE_HOLIDAYS_FY2026` set in eda.py — hardcoded dates for FY 2025-26 (NSE publishes this annually). When checking expected trading days, exclude holidays. Label them in the report as "Holiday" not "Missing".

Known NSE holidays in the data range (Mar 2026):
- 2026-03-26 (Holi)
- 2026-03-30 is expiry day — if pipeline runs before expiry, it's "Future" not "Missing"

### 2. ATM-Relative Completeness

Stop cross-joining all strikes x all days. Instead:
- Per day: count how many strikes have data. Expect 9 (ATM-4 to ATM+4).
- Per day: count total candles. Expect ~6750 (9 strikes x 2 types x 375 min).
- Warn only if strikes_per_day < 9 or candles_per_day < 6000.
- Show compact "Strike Coverage" table: date | strikes | candles | ATM strike | status.
- Remove the verbose Missing Combos table entirely.

### 3. Lenient Outlier Thresholds

- Volume/OI: 5x IQR (up from 3x) — options volume is inherently heavy-tailed
- Other fields: keep 3x IQR
- Time gaps: warn only if avg gaps per group > 10 (a few gaps per day is normal)

### 4. Post-EDA Health Check

Add `_check_health(completeness, nulls, ohlc)` in eda.py called at end of `run_eda`:
- Returns: "HEALTHY" / "DEGRADED" / "BROKEN"
- BROKEN: total candles = 0, or critical nulls > 0, or OHLC violations > 1%
- DEGRADED: any day with < 6000 candles, or outliers > 20%
- HEALTHY: everything else
- Logs a single summary line: `EDA HEALTH: HEALTHY | 60676 candles, 9 days, 0 nulls`

### 5. Move EDA Into _run_expiry

Move the EDA call from the post-pipeline block in `run_pipeline()` (lines 273-287) into `_run_expiry()`, right after `insert_output()`. Each expiry gets its own EDA immediately after its output is written. Remove the separate post-loop EDA block.

## Files Changed

| File | Change |
|------|--------|
| `tests/supabase/eda.py` | Holiday calendar, ATM-relative completeness, lenient thresholds, health check, compact report |
| `DhanHQ_src/main.py` | Move EDA into `_run_expiry` after `insert_output` |
