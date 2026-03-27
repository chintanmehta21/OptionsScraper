"""Supabase EDA — single HTML report with stats, charts, tables.

Outputs to docs/logs/supabase/eda_{DDMMYYYY}/report.html
Run standalone:  python -m tests.supabase.eda
"""

import base64
import io
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, date as dt_date, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from DhanHQ_src.config import (
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
    EXPIRY_DATE, FROM_DATE, TO_DATE, STRIKES,
)
from supabase import create_client

logger = logging.getLogger(__name__)

NUMERIC_FIELDS = ["open", "high", "low", "close", "volume", "oi", "iv", "spot"]
EXPECTED_STRIKES = len(STRIKES)  # 9
OPTION_TYPES = ["CE", "PE"]


# ── Helpers ────────────────────────────────────────────────────

def _get_client(url=None, key=None):
    return create_client(url or SUPABASE_URL, key or SUPABASE_SERVICE_ROLE_KEY)


def _fetch_all(client, table, select="*", filters=None, order="timestamp"):
    """Paginated fetch, 1000 per page."""
    rows, page, offset = [], 1000, 0
    while True:
        q = client.table(table).select(select).order(order)
        if filters:
            for col, op, val in filters:
                q = getattr(q, op)(col, val)
        resp = q.range(offset, offset + page - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < page:
            break
        offset += page
    return rows


def _expected_trading_days(from_date, to_date):
    """Generate weekday dates in [from_date, to_date) — approximates trading days."""
    start = dt_date.fromisoformat(from_date)
    end = dt_date.fromisoformat(to_date)
    days = []
    d = start
    while d < end:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def _sid_filter(strike_ids):
    return f"({','.join(str(s) for s in strike_ids)})"


def _q1_q3(values):
    n = len(values)
    return values[n // 4], values[3 * n // 4]


def _fig_to_base64(fig):
    """Convert matplotlib figure to base64 PNG for HTML embedding."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight", facecolor="#1e1e2e")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    buf.close()
    import matplotlib.pyplot as plt
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


# ── Data collection (single pass) ─────────────────────────────

def _load_data(client, expiry_id):
    """Load strikes + raw_candles in one go, return (strikes, candles)."""
    strikes = client.table("strikes").select(
        "id, strike, atm_offset"
    ).eq("expiry_id", expiry_id).order("strike").execute().data

    if not strikes:
        return [], []

    sids = _sid_filter([s["id"] for s in strikes])
    candles = _fetch_all(
        client, "raw_candles",
        select="id, strike_id, option_type, timestamp, open, high, low, close, volume, oi, iv, spot",
        filters=[("strike_id", "in_", sids)],
    )
    return strikes, candles


# ── Checks ─────────────────────────────────────────────────────

def check_completeness(strikes, candles, from_date, to_date, expiry_date):
    """Thorough completeness: expected trading days x strikes x CE/PE.

    Returns dict with per-combo candle counts, missing combos, heatmap data.
    """
    expected_days = _expected_trading_days(from_date, to_date)
    strike_map = {s["id"]: s for s in strikes}
    strike_labels = sorted(set(s["strike"] for s in strikes))

    # Count candles per (strike, option_type, date)
    counts = defaultdict(int)
    actual_dates = set()
    for c in candles:
        d = c["timestamp"][:10]
        actual_dates.add(d)
        sid = c["strike_id"]
        s_info = strike_map.get(sid)
        if s_info:
            counts[(s_info["strike"], c["option_type"], d)] += 1

    # Dates present in data but not in expected (e.g. holidays that had no trading)
    extra_dates = sorted(actual_dates - set(expected_days))
    # Expected dates with no data at all
    missing_dates = sorted(set(expected_days) - actual_dates)
    # Use actual trading days (intersection) for the heatmap
    trading_days = sorted(actual_dates & set(expected_days))
    # Also include any actual dates not in expected (data exists)
    all_data_dates = sorted(actual_dates)

    # Build heatmap: rows=strikes, cols=dates, cells=candle count
    heatmap = {}  # (strike, option_type) -> {date: count}
    missing_combos = []
    for strike in strike_labels:
        for ot in OPTION_TYPES:
            row = {}
            for d in all_data_dates:
                cnt = counts.get((strike, ot, d), 0)
                row[d] = cnt
                if cnt == 0 and d in set(expected_days):
                    missing_combos.append({"strike": strike, "type": ot, "date": d})
            heatmap[(strike, ot)] = row

    # Per-day total candle stats
    day_totals = {}
    for d in all_data_dates:
        total = sum(counts.get((s, ot, d), 0) for s in strike_labels for ot in OPTION_TYPES)
        day_totals[d] = total

    return {
        "expected_days": expected_days,
        "actual_days": all_data_dates,
        "missing_dates": missing_dates,
        "extra_dates": extra_dates,
        "expected_combos": len(strike_labels) * len(OPTION_TYPES) * len(expected_days),
        "populated_combos": sum(1 for v in counts.values() if v > 0),
        "missing_combos": missing_combos,
        "heatmap": heatmap,
        "day_totals": day_totals,
        "strike_labels": strike_labels,
        "total_candles": len(candles),
    }


def check_nulls(candles):
    """Null counts per field."""
    total = len(candles)
    null_counts = {f: 0 for f in NUMERIC_FIELDS}
    for row in candles:
        for f in NUMERIC_FIELDS:
            if row.get(f) is None:
                null_counts[f] += 1
    return {"total_rows": total, "null_counts": null_counts}


def check_ohlc_integrity(candles):
    """OHLC constraint violations: L <= O,C <= H."""
    violations = []
    for row in candles:
        o, h, l, c = row.get("open"), row.get("high"), row.get("low"), row.get("close")
        if None in (o, h, l, c):
            continue
        errs = []
        if l > h:
            errs.append(f"L({l})>H({h})")
        if l > min(o, c):
            errs.append(f"L({l})>min(O,C)")
        if h < max(o, c):
            errs.append(f"H({h})<max(O,C)")
        if errs:
            violations.append({
                "id": row["id"], "ts": row["timestamp"],
                "ohlc": [o, h, l, c], "errs": errs,
            })
    return {"checked": len(candles), "violations": violations}


def check_time_gaps(strikes, candles):
    """Gaps > 1 min within a trading day for any strike/type combo."""
    strike_map = {s["id"]: s for s in strikes}
    groups = defaultdict(list)
    for c in candles:
        sid = c["strike_id"]
        s_info = strike_map.get(sid)
        if not s_info:
            continue
        d = c["timestamp"][:10]
        hm = c["timestamp"][11:16]
        groups[(s_info["strike"], c["option_type"], d)].append(hm)

    gaps = []
    candles_per_group = []
    for (strike, ot, d), times in groups.items():
        ts = sorted(times)
        candles_per_group.append(len(ts))
        for i in range(1, len(ts)):
            ph, pm = map(int, ts[i - 1].split(":"))
            ch, cm = map(int, ts[i].split(":"))
            gap = (ch * 60 + cm) - (ph * 60 + pm)
            if gap > 1:
                gaps.append({
                    "strike": strike, "type": ot, "date": d,
                    "after": ts[i - 1], "before": ts[i], "gap_min": gap,
                })

    return {"groups": len(groups), "gaps": gaps, "candles_per_group": candles_per_group}


def check_outliers(candles):
    """IQR-based outlier detection (3x IQR), per field."""
    result = {}
    for field in NUMERIC_FIELDS:
        vals = sorted(v[field] for v in candles if v.get(field) is not None)
        if len(vals) < 20:
            continue
        q1, q3 = _q1_q3(vals)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
        outliers = [v for v in vals if v < lo or v > hi]
        result[field] = {
            "n": len(vals), "q1": round(q1, 2), "q3": round(q3, 2),
            "iqr": round(iqr, 2), "lo": round(lo, 2), "hi": round(hi, 2),
            "outlier_count": len(outliers),
            "pct": round(len(outliers) / len(vals) * 100, 2),
            "min": round(vals[0], 2), "max": round(vals[-1], 2),
            "median": round(vals[len(vals) // 2], 2),
            "mean": round(sum(vals) / len(vals), 2),
        }
    return result


def check_field_distributions(candles):
    """Basic stats per numeric field for the stats table."""
    result = {}
    for field in NUMERIC_FIELDS:
        vals = sorted(v[field] for v in candles if v.get(field) is not None)
        if not vals:
            result[field] = {"n": 0}
            continue
        n = len(vals)
        result[field] = {
            "n": n, "min": round(vals[0], 2), "max": round(vals[-1], 2),
            "mean": round(sum(vals) / n, 2), "median": round(vals[n // 2], 2),
            "p5": round(vals[int(n * 0.05)], 2), "p95": round(vals[int(n * 0.95)], 2),
        }
    return result


# ── Chart generation ───────────────────────────────────────────

def _make_charts(completeness, nulls, outliers, time_gaps, candles, strikes):
    """Generate matplotlib charts, return dict of {name: base64_png}."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
    except ImportError:
        logger.warning("matplotlib not installed, skipping charts")
        return {}

    charts = {}
    dark_bg = "#1e1e2e"
    text_col = "#cdd6f4"
    accent = "#89b4fa"
    warn_col = "#f38ba8"
    ok_col = "#a6e3a1"

    plt.rcParams.update({
        "figure.facecolor": dark_bg, "axes.facecolor": "#313244",
        "axes.edgecolor": "#585b70", "text.color": text_col,
        "xtick.color": text_col, "ytick.color": text_col,
        "axes.labelcolor": text_col, "font.size": 9,
    })

    # 1. Completeness heatmap
    hm = completeness["heatmap"]
    strike_labels = completeness["strike_labels"]
    dates = completeness["actual_days"]
    if strike_labels and dates:
        row_labels = []
        data_matrix = []
        for strike in strike_labels:
            for ot in OPTION_TYPES:
                row_labels.append(f"{strike} {ot}")
                row = [hm.get((strike, ot), {}).get(d, 0) for d in dates]
                data_matrix.append(row)

        fig, ax = plt.subplots(figsize=(max(8, len(dates) * 0.6), max(4, len(row_labels) * 0.3)))
        cmap = mcolors.LinearSegmentedColormap.from_list("rg", [warn_col, "#fab387", ok_col])
        max_val = max(max(r) for r in data_matrix) if data_matrix and any(data_matrix) else 1
        im = ax.imshow(data_matrix, aspect="auto", cmap=cmap, vmin=0, vmax=max(max_val, 1))
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, ha="right", fontsize=7)
        ax.set_yticks(range(len(row_labels)))
        ax.set_yticklabels(row_labels, fontsize=7)
        ax.set_title("Candle Count per Strike/Type/Date", fontsize=11, pad=10)
        fig.colorbar(im, ax=ax, shrink=0.6, label="candles")
        charts["completeness_heatmap"] = _fig_to_base64(fig)

    # 2. Candles per day bar chart
    day_totals = completeness["day_totals"]
    if day_totals:
        fig, ax = plt.subplots(figsize=(max(6, len(day_totals) * 0.5), 3.5))
        days = list(day_totals.keys())
        vals = [day_totals[d] for d in days]
        colors = [ok_col if v > 0 else warn_col for v in vals]
        ax.bar(range(len(days)), vals, color=colors, edgecolor="#585b70", linewidth=0.5)
        ax.set_xticks(range(len(days)))
        ax.set_xticklabels([d[5:] for d in days], rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Total candles")
        ax.set_title("Candles per Trading Day", fontsize=11, pad=10)
        charts["candles_per_day"] = _fig_to_base64(fig)

    # 3. Null counts bar
    nc = nulls["null_counts"]
    fields_with_nulls = {k: v for k, v in nc.items() if v > 0}
    if fields_with_nulls:
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.barh(list(fields_with_nulls.keys()), list(fields_with_nulls.values()),
                color=warn_col, edgecolor="#585b70")
        ax.set_xlabel("Null count")
        ax.set_title("Null Values by Field", fontsize=11, pad=10)
        charts["null_counts"] = _fig_to_base64(fig)

    # 4. Outlier % bar
    if outliers:
        fields = [f for f in outliers if outliers[f]["outlier_count"] > 0]
        if fields:
            fig, ax = plt.subplots(figsize=(5, 3))
            pcts = [outliers[f]["pct"] for f in fields]
            ax.barh(fields, pcts, color="#fab387", edgecolor="#585b70")
            ax.set_xlabel("Outlier %")
            ax.set_title("Outliers by Field (3x IQR)", fontsize=11, pad=10)
            charts["outlier_pct"] = _fig_to_base64(fig)

    # 5. Gap distribution histogram
    gaps = time_gaps["gaps"]
    if gaps:
        gap_mins = [g["gap_min"] for g in gaps]
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.hist(gap_mins, bins=min(30, len(set(gap_mins))), color=accent, edgecolor="#585b70")
        ax.set_xlabel("Gap (minutes)")
        ax.set_ylabel("Frequency")
        ax.set_title("Time Gap Distribution", fontsize=11, pad=10)
        charts["gap_histogram"] = _fig_to_base64(fig)

    # 6. Candles-per-group distribution
    cpg = time_gaps["candles_per_group"]
    if cpg:
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.hist(cpg, bins=min(30, len(set(cpg))), color=accent, edgecolor="#585b70")
        ax.set_xlabel("Candles in group (strike/type/day)")
        ax.set_ylabel("Frequency")
        ax.set_title("Candle Count Distribution per Group", fontsize=11, pad=10)
        charts["candles_per_group"] = _fig_to_base64(fig)

    return charts


# ── HTML report ────────────────────────────────────────────────

_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: #1e1e2e; color: #cdd6f4;
       max-width: 1100px; margin: 0 auto; padding: 24px; line-height: 1.5; }
h1 { font-size: 1.6em; margin-bottom: 4px; color: #89b4fa; }
h2 { font-size: 1.2em; margin: 28px 0 10px; color: #cba6f7; border-bottom: 1px solid #45475a; padding-bottom: 4px; }
h3 { font-size: 1em; margin: 16px 0 6px; color: #f5c2e7; }
.meta { color: #a6adc8; font-size: 0.85em; margin-bottom: 20px; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 4px; font-size: 0.8em; font-weight: 600; margin-right: 6px; }
.ok { background: #a6e3a1; color: #1e1e2e; }
.warn { background: #f38ba8; color: #1e1e2e; }
.info { background: #89b4fa; color: #1e1e2e; }
.card { background: #313244; border-radius: 8px; padding: 16px; margin: 12px 0; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin: 12px 0; }
.stat-box { background: #45475a; border-radius: 6px; padding: 12px; text-align: center; }
.stat-box .num { font-size: 1.8em; font-weight: 700; color: #89b4fa; }
.stat-box .label { font-size: 0.8em; color: #a6adc8; }
table { width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 0.85em; }
th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid #45475a; }
th { background: #45475a; color: #cdd6f4; font-weight: 600; }
tr:hover { background: #45475a44; }
.chart-container { margin: 12px 0; text-align: center; }
.chart-container img { max-width: 100%; border-radius: 6px; }
.missing { color: #f38ba8; }
.good { color: #a6e3a1; }
.section-verdict { margin: 6px 0; font-weight: 600; }
"""


def _html_table(headers, rows, max_rows=50):
    """Build HTML table from headers list and list-of-lists rows."""
    h = "<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead><tbody>"
    for row in rows[:max_rows]:
        h += "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
    if len(rows) > max_rows:
        h += f'<tr><td colspan="{len(headers)}" style="text-align:center;color:#a6adc8;">... {len(rows) - max_rows} more rows</td></tr>'
    h += "</tbody></table>"
    return h


def build_html(completeness, nulls, ohlc, time_gaps, outliers, distributions, charts, expiry_date):
    """Build the full single-page HTML report, sections ordered by importance."""
    today = datetime.now().strftime("%d %b %Y, %H:%M IST")

    # Compute verdicts
    total_checks = 6
    fails = 0
    verdicts = {}

    # Completeness verdict
    mc = len(completeness["missing_combos"])
    md = len(completeness["missing_dates"])
    if mc > 0 or md > 0:
        verdicts["completeness"] = "warn"
        fails += 1
    else:
        verdicts["completeness"] = "ok"

    # Nulls
    total_nulls = sum(nulls["null_counts"].values())
    critical_nulls = sum(nulls["null_counts"].get(f, 0) for f in ["open", "high", "low", "close"])
    if critical_nulls > 0:
        verdicts["nulls"] = "warn"
        fails += 1
    else:
        verdicts["nulls"] = "ok"

    # OHLC
    if ohlc["violations"]:
        verdicts["ohlc"] = "warn"
        fails += 1
    else:
        verdicts["ohlc"] = "ok"

    # Time gaps
    if time_gaps["gaps"]:
        verdicts["gaps"] = "warn"
        fails += 1
    else:
        verdicts["gaps"] = "ok"

    # Outliers
    any_outliers = any(outliers[f]["outlier_count"] > 0 for f in outliers)
    verdicts["outliers"] = "warn" if any_outliers else "ok"
    if any_outliers:
        fails += 1

    # Derived (informational)
    verdicts["distributions"] = "ok"

    passes = total_checks - fails
    overall = "PASS" if fails == 0 else "WARN"
    overall_class = "ok" if fails == 0 else "warn"

    parts = []
    parts.append(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<title>EDA Report — {expiry_date}</title><style>{_CSS}</style></head><body>
<h1>NIFTY Options EDA Report</h1>
<div class="meta">Expiry: {expiry_date} | Generated: {today} | Data range: {completeness['expected_days'][0] if completeness['expected_days'] else '?'} to {completeness['expected_days'][-1] if completeness['expected_days'] else '?'}</div>
""")

    # ── 1. Executive Summary ──────────────────────────────
    parts.append(f"""<h2>1. Executive Summary</h2>
<div class="card">
<span class="badge {overall_class}">{overall}</span> {passes}/{total_checks} checks passed, {fails} warnings
<div class="summary-grid">
  <div class="stat-box"><div class="num">{completeness['total_candles']:,}</div><div class="label">Total Candles</div></div>
  <div class="stat-box"><div class="num">{len(completeness['actual_days'])}</div><div class="label">Trading Days</div></div>
  <div class="stat-box"><div class="num">{len(completeness['strike_labels'])}</div><div class="label">Strikes</div></div>
  <div class="stat-box"><div class="num">{nulls['total_rows']:,}</div><div class="label">Rows Scanned</div></div>
</div>
<table><thead><tr><th>Check</th><th>Status</th><th>Detail</th></tr></thead><tbody>
""")
    check_details = [
        ("Data Completeness", verdicts["completeness"],
         f"{mc} missing combos, {md} missing dates" if verdicts["completeness"] == "warn" else "All strike/type/date combos present"),
        ("Null Values", verdicts["nulls"],
         f"{total_nulls} nulls ({critical_nulls} in OHLC)" if total_nulls > 0 else "No nulls"),
        ("OHLC Integrity", verdicts["ohlc"],
         f"{len(ohlc['violations'])} violations" if ohlc["violations"] else "All OHLC valid"),
        ("Time Gaps", verdicts["gaps"],
         f"{len(time_gaps['gaps'])} gaps >1min" if time_gaps["gaps"] else "No gaps"),
        ("Outliers", verdicts["outliers"],
         f"Detected in {sum(1 for f in outliers if outliers[f]['outlier_count'] > 0)} fields" if any_outliers else "None (3x IQR)"),
        ("Field Distributions", "ok", "Informational"),
    ]
    for name, v, detail in check_details:
        badge = "ok" if v == "ok" else "warn"
        parts.append(f'<tr><td>{name}</td><td><span class="badge {badge}">{"PASS" if v == "ok" else "WARN"}</span></td><td>{detail}</td></tr>')
    parts.append("</tbody></table></div>")

    # ── 2. Data Completeness ──────────────────────────────
    parts.append("<h2>2. Data Completeness</h2><div class='card'>")
    parts.append(f"""<p>Expected date range: <b>{completeness['expected_days'][0] if completeness['expected_days'] else '?'}</b> to
<b>{completeness['expected_days'][-1] if completeness['expected_days'] else '?'}</b>
({len(completeness['expected_days'])} weekdays) |
Actual data on <b>{len(completeness['actual_days'])}</b> days |
{EXPECTED_STRIKES} strikes x 2 types = {EXPECTED_STRIKES * 2} combos/day</p>""")

    if completeness["missing_dates"]:
        parts.append(f'<p class="missing">Missing dates (no data at all): {", ".join(completeness["missing_dates"])}</p>')
    else:
        parts.append('<p class="good">All expected trading days have data.</p>')

    if charts.get("completeness_heatmap"):
        parts.append(f'<div class="chart-container"><img src="{charts["completeness_heatmap"]}" alt="Completeness heatmap"></div>')

    if charts.get("candles_per_day"):
        parts.append(f'<div class="chart-container"><img src="{charts["candles_per_day"]}" alt="Candles per day"></div>')

    # Day totals table
    if completeness["day_totals"]:
        rows = [[d, f"{v:,}"] for d, v in completeness["day_totals"].items()]
        parts.append("<h3>Candles per Day</h3>")
        parts.append(_html_table(["Date", "Total Candles"], rows))

    if completeness["missing_combos"]:
        parts.append(f"<h3>Missing Combos ({len(completeness['missing_combos'])})</h3>")
        mc_rows = [[m["strike"], m["type"], m["date"]] for m in completeness["missing_combos"]]
        parts.append(_html_table(["Strike", "Type", "Date"], mc_rows))
    parts.append("</div>")

    # ── 3. Null Analysis ──────────────────────────────────
    parts.append("<h2>3. Null Values</h2><div class='card'>")
    if total_nulls == 0:
        parts.append('<p class="good">No null values in any numeric field.</p>')
    else:
        if charts.get("null_counts"):
            parts.append(f'<div class="chart-container"><img src="{charts["null_counts"]}" alt="Null counts"></div>')
        nc_rows = [[f, f"{nulls['null_counts'][f]:,}",
                     f"{nulls['null_counts'][f] / max(nulls['total_rows'], 1) * 100:.2f}%"]
                    for f in NUMERIC_FIELDS if nulls["null_counts"][f] > 0]
        parts.append(_html_table(["Field", "Null Count", "% of Rows"], nc_rows))
    parts.append("</div>")

    # ── 4. OHLC Integrity ─────────────────────────────────
    parts.append("<h2>4. OHLC Integrity</h2><div class='card'>")
    parts.append(f"<p>Checked {ohlc['checked']:,} rows for L &le; O,C &le; H constraint.</p>")
    if not ohlc["violations"]:
        parts.append('<p class="good">All rows pass OHLC integrity.</p>')
    else:
        parts.append(f'<p class="missing">{len(ohlc["violations"])} violations found.</p>')
        v_rows = [[v["ts"], v["ohlc"], ", ".join(v["errs"])] for v in ohlc["violations"][:30]]
        parts.append(_html_table(["Timestamp", "OHLC", "Violations"], v_rows))
    parts.append("</div>")

    # ── 5. Time Gaps ──────────────────────────────────────
    parts.append("<h2>5. Time Gaps</h2><div class='card'>")
    parts.append(f"<p>{time_gaps['groups']:,} (strike/type/day) groups analyzed.</p>")
    if not time_gaps["gaps"]:
        parts.append('<p class="good">No gaps >1 minute found.</p>')
    else:
        parts.append(f'<p class="missing">{len(time_gaps["gaps"])} gaps found.</p>')
        if charts.get("gap_histogram"):
            parts.append(f'<div class="chart-container"><img src="{charts["gap_histogram"]}" alt="Gap distribution"></div>')
        g_rows = [[g["strike"], g["type"], g["date"], g["after"], g["before"], g["gap_min"]]
                   for g in time_gaps["gaps"][:40]]
        parts.append(_html_table(["Strike", "Type", "Date", "After", "Before", "Gap (min)"], g_rows))

    if charts.get("candles_per_group"):
        parts.append(f'<h3>Candle Count Distribution</h3><div class="chart-container"><img src="{charts["candles_per_group"]}" alt="Candles per group"></div>')
    parts.append("</div>")

    # ── 6. Outliers ───────────────────────────────────────
    parts.append("<h2>6. Outliers (3x IQR)</h2><div class='card'>")
    if not any_outliers:
        parts.append('<p class="good">No outliers detected.</p>')
    else:
        if charts.get("outlier_pct"):
            parts.append(f'<div class="chart-container"><img src="{charts["outlier_pct"]}" alt="Outlier %"></div>')
        o_rows = [[f, outliers[f]["outlier_count"], f"{outliers[f]['pct']}%",
                    f"[{outliers[f]['lo']}, {outliers[f]['hi']}]",
                    f"{outliers[f]['min']} — {outliers[f]['max']}"]
                   for f in outliers if outliers[f]["outlier_count"] > 0]
        parts.append(_html_table(["Field", "Count", "%", "IQR Bounds", "Range"], o_rows))
    parts.append("</div>")

    # ── 7. Field Distributions ────────────────────────────
    parts.append("<h2>7. Field Distributions</h2><div class='card'>")
    d_rows = [[f, distributions[f].get("n", 0),
                distributions[f].get("min", ""), distributions[f].get("p5", ""),
                distributions[f].get("median", ""), distributions[f].get("mean", ""),
                distributions[f].get("p95", ""), distributions[f].get("max", "")]
               for f in NUMERIC_FIELDS if distributions.get(f, {}).get("n", 0) > 0]
    parts.append(_html_table(["Field", "N", "Min", "P5", "Median", "Mean", "P95", "Max"], d_rows))
    parts.append("</div>")

    parts.append("</body></html>")
    return "\n".join(parts)


# ── Entry point ────────────────────────────────────────────────

def run_eda(url=None, key=None, expiry_date=None, output_dir=None):
    """Run all checks and generate HTML report. Returns output dir."""
    client = _get_client(url, key)
    expiry = expiry_date or EXPIRY_DATE

    resp = client.table("expiries").select("id").eq(
        "symbol", "NIFTY"
    ).eq("expiry_date", expiry).execute()
    if not resp.data:
        logger.error("No expiry found for NIFTY %s", expiry)
        return None
    expiry_id = resp.data[0]["id"]

    today = datetime.now().strftime("%d%m%Y")
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT, "docs", "logs", "supabase", f"eda_{today}")
    os.makedirs(output_dir, exist_ok=True)

    logger.info("EDA for expiry %s (id=%d) -> %s", expiry, expiry_id, output_dir)

    # Single data load
    strikes, candles = _load_data(client, expiry_id)
    if not candles:
        logger.error("No candle data found")
        return None

    logger.info("  Loaded %d strikes, %d candles", len(strikes), len(candles))

    # Run checks
    completeness = check_completeness(strikes, candles, FROM_DATE, TO_DATE, expiry)
    nulls = check_nulls(candles)
    ohlc = check_ohlc_integrity(candles)
    time_gaps_result = check_time_gaps(strikes, candles)
    outliers = check_outliers(candles)
    distributions = check_field_distributions(candles)

    # Charts
    charts = _make_charts(completeness, nulls, outliers, time_gaps_result, candles, strikes)

    # HTML
    html = build_html(completeness, nulls, ohlc, time_gaps_result, outliers, distributions, charts, expiry)
    report_path = os.path.join(output_dir, "report.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Also dump raw JSON for programmatic use
    raw = {
        "completeness": {k: v for k, v in completeness.items() if k != "heatmap"},
        "nulls": nulls,
        "ohlc": {"checked": ohlc["checked"], "violation_count": len(ohlc["violations"])},
        "time_gaps": {"groups": time_gaps_result["groups"], "gap_count": len(time_gaps_result["gaps"])},
        "outliers": outliers,
        "distributions": distributions,
    }
    with open(os.path.join(output_dir, "raw_data.json"), "w") as f:
        json.dump(raw, f, indent=2, default=str)

    # Print summary
    fails = sum(1 for v in [
        len(completeness["missing_combos"]) > 0 or len(completeness["missing_dates"]) > 0,
        sum(nulls["null_counts"].get(f, 0) for f in ["open", "high", "low", "close"]) > 0,
        len(ohlc["violations"]) > 0,
        len(time_gaps_result["gaps"]) > 0,
        any(outliers[f]["outlier_count"] > 0 for f in outliers),
    ] if v)
    status = "PASS" if fails == 0 else f"WARN ({fails} issues)"
    print(f"\n  EDA: {status} | {len(candles):,} candles, {len(completeness['actual_days'])} days")
    print(f"  Report: {report_path}\n")

    return output_dir


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_eda()
