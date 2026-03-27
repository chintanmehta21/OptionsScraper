# Fix DhanHQ API Response Parsing

## Problem

DhanHQ rolling option API now returns nested format:
```json
{"data": {"ce": {"timestamp": [...], "iv": [...], ...}, "pe": null}}
```

But `parse_api_response()` expects flat `{"timestamp": [...], "open": [...]}`. Both IV baseline (0 entries) and options data (0 candles) fail to parse. Pipeline exits 0 despite producing no data.

## Design

### 1. Unwrap nested response in `fetch_with_retry`

After getting the API response, detect the nested `data.ce`/`data.pe` format. Extract the correct inner object based on `kwargs["drv_option_type"]`:
- `"CALL"` → `response["data"]["ce"]`
- `"PUT"` → `response["data"]["pe"]`

If the old flat format is returned (has `timestamp` at top level), pass through unchanged (backward compat).

`parse_api_response()` stays untouched — the inner objects have the same structure it already expects.

### 2. Pipeline exit code

`main.py` must `sys.exit(1)` when `all_stats` is empty, so CI workflow properly fails.

## Files changed

| File | Change |
|------|--------|
| `DhanHQ_src/fetcher.py:103-128` | Add nested response detection + extraction in `fetch_with_retry` |
| `DhanHQ_src/main.py:299-301` | Exit 1 when no data produced |

## Success criteria

1. Options data parsed correctly — non-zero candle count
2. IV baseline parsed — non-zero daily entries
3. Pipeline exits 1 on failure, 0 on success
4. Supabase output table populated for 2026-03-30
