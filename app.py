"""
FieldInsight Ops Dashboard — Flask backend  v2.0
=================================================
Architecture:
  · Pipeline / SDR data  → NocoDB  (synced nightly from HubSpot by Alfred)
  · Researcher activity  → Google Sheets
  · SDR call activity    → Google Sheets
  · AE / Sales pipeline  → HubSpot (Natasha + Main — small dataset, 1 call)
  · Onboarding           → Google Sheets

HubSpot is NEVER queried for bulk deal data from here.
"""

import os
import json
import time
import base64
import logging
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from functools import wraps
from flask import Flask, jsonify, request, render_template, abort
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ── Config ────────────────────────────────────────────────────────────────────

HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY", "")
SALES_SHEET_ID    = os.getenv("SALES_SHEET_ID", "1qN420Yk7RFXUFLfmT9xecWGBYuM0TEhBSvBgi2kH_ng")
DASHBOARD_SECRET  = os.getenv("DASHBOARD_SECRET", "")
CACHE_TTL_SECS    = int(os.getenv("CACHE_TTL", "1800"))
GOOGLE_SA_B64     = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SA_PATH    = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "")
NOCO_BASE_URL     = os.getenv("NOCODB_BASE_URL", "https://app.nocodb.com")
NOCO_TOKEN        = os.getenv("NOCODB_TOKEN", "")
NOCO_PROJECT_ID   = os.getenv("NOCODB_PROJECT_ID", "pmnqbv38yudws3v")
NOCO_DEALS_TABLE  = os.getenv("NOCODB_DEALS_TABLE_ID", "migi5lc0fb9zhie")

# ── In-memory cache ───────────────────────────────────────────────────────────

_cache: dict = {}

def _cached(key: str, ttl: int = CACHE_TTL_SECS):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = time.time()
            if key in _cache and now - _cache[key]["ts"] < ttl:
                return _cache[key]["data"]
            result = fn(*args, **kwargs)
            _cache[key] = {"ts": now, "data": result}
            return result
        return wrapper
    return decorator

def _bust_cache(key: str):
    _cache.pop(key, None)

# ── NocoDB client ─────────────────────────────────────────────────────────────

def _noco_headers():
    return {"xc-token": NOCO_TOKEN, "Content-Type": "application/json"}

def _noco_get_all(table_id: str, where: str = "", fields: str = "") -> list:
    """Fetch ALL rows from a NocoDB table (handles pagination; NocoDB caps at 100/page)."""
    rows = []
    page = 1
    PAGE_SIZE = 100   # NocoDB hard cap per page
    while True:
        params = {"limit": PAGE_SIZE, "offset": (page - 1) * PAGE_SIZE}
        if where:  params["where"]  = where
        if fields: params["fields"] = fields
        r = requests.get(
            f"{NOCO_BASE_URL}/api/v1/db/data/noco/{NOCO_PROJECT_ID}/{table_id}",
            headers=_noco_headers(), params=params,
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("list", [])
        rows.extend(batch)
        page_info = data.get("pageInfo", {})
        if page_info.get("isLastPage", True) or len(batch) < PAGE_SIZE:
            break
        page += 1
    log.info(f"NocoDB: loaded {len(rows)} rows from {table_id}")
    return rows

# ── HubSpot client (only used for AE/Sales pipeline) ─────────────────────────

HS_BASE = "https://api.hubapi.com"

def _hs_headers():
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}

def _hs_post(path, body, max_retries=4):
    time.sleep(0.15)
    for attempt in range(max_retries):
        r = requests.post(f"{HS_BASE}{path}", headers=_hs_headers(), json=body)
        if r.status_code in (429, 500, 502, 503):
            wait = (2 ** attempt) * 2
            log.warning(f"HubSpot {r.status_code} → retry in {wait}s")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

# ── Pipeline / stage constants ────────────────────────────────────────────────

PIPELINE_MAIN   = "856313360"
PIPELINE_NATASHA = "default"
WON_STAGE_IDS   = {"810a68c2-e01d-436b-aaa9-7c81f0760ec5", "1277058739"}
LOST_STAGE_IDS  = {"28103468", "1277058740"}

NATASHA_STAGES = {
    "17906279":"Qualification Req","34701073":"Demo Scheduled",
    "f4aca0be-7a50-46f7-9a33-c916ecbc17f6":"Demo Attended",
    "100113677":"Closeable/WIP","113846371":"Proposal Sent",
    "810a68c2-e01d-436b-aaa9-7c81f0760ec5":"WON","28103468":"LOST",
    "1277058739":"WON","1277058740":"LOST",
}

KNOWN_RESEARCHERS = ["Tamil","Veera","Mohanapriya","Chinju","Mark Lang","Tim","Jay","Tash","Paul"]

# ── Google Sheets client ──────────────────────────────────────────────────────

def _get_sheets_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    if GOOGLE_SA_B64:
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64).decode())
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    elif GOOGLE_SA_PATH:
        creds = Credentials.from_service_account_file(GOOGLE_SA_PATH, scopes=scopes)
    else:
        raise ValueError("No Google service account configured.")
    return gspread.authorize(creds)

def _read_sheet_tab(spreadsheet_id, tab_name, max_rows=600):
    gc = _get_sheets_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab_name)
    return ws.get_all_values()[:max_rows]

# ── Data: Pipeline Quality (reads from NocoDB) ────────────────────────────────

@_cached("pipeline_quality", ttl=1800)
def _fetch_pipeline_quality():
    deals = _noco_get_all(NOCO_DEALS_TABLE)

    stage_counts = Counter()
    by_researcher = defaultdict(list)

    for d in deals:
        stage_counts[d.get("stage","") or "Unknown"] += 1
        researcher = (d.get("researcher","") or "Not set").strip() or "Not set"
        by_researcher[researcher].append(d)

    # Researcher scorecard
    researcher_stats = []
    for researcher in (KNOWN_RESEARCHERS + ["Not set"]):
        rdl = by_researcher.get(researcher)
        if not rdl: continue
        total  = len(rdl)
        active = sum(1 for d in rdl if not d.get("is_disqualified"))
        no_mob = sum(1 for d in rdl if d.get("is_no_mobile"))
        demos  = sum(1 for d in rdl if d.get("is_demo_booked"))
        won    = sum(1 for d in rdl if d.get("is_won"))
        icp_ab = sum(1 for d in rdl if (d.get("icp_tier","") or "") in ("A","B"))
        phone_ok = active - no_mob
        researcher_stats.append({
            "name":      researcher,
            "total":     total,
            "active":    active,
            "no_mobile": no_mob,
            "phone_pct": round(phone_ok/active*100) if active else 0,
            "demos":     demos,
            "won":       won,
            "icp_ab":    icp_ab,
            "icp_pct":   round(icp_ab/total*100) if total else 0,
            "demo_rate": round(demos/active*100) if active else 0,
            "win_rate":  round(won/total*100) if total else 0,
        })

    total = len(deals)
    active = sum(1 for d in deals if not d.get("is_disqualified"))

    # Get last synced_at
    synced_ats = [d.get("synced_at","") for d in deals if d.get("synced_at")]
    last_sync  = max(synced_ats) if synced_ats else "Never"

    return {
        "summary": {
            "total":         total,
            "active":        active,
            "disqualified":  total - active,
            "demo_booked":   sum(1 for d in deals if d.get("is_demo_booked")),
            "no_mobile":     sum(1 for d in deals if d.get("is_no_mobile")),
            "won":           sum(1 for d in deals if d.get("is_won")),
            "icp_ab":        sum(1 for d in deals if (d.get("icp_tier","") or "") in ("A","B")),
            "no_mobile_pct": round(sum(1 for d in deals if d.get("is_no_mobile"))/total*100) if total else 0,
            "last_hs_sync":  last_sync,
        },
        "by_researcher": researcher_stats,
        "stage_counts":  dict(stage_counts.most_common(14)),
        "won_deals":     sorted(
            [{"company": d.get("company_name",""), "researcher": d.get("researcher",""),
              "sdr": d.get("sdr",""), "sdr_demo": d.get("sdr_demo_booked_by",""),
              "won_at": d.get("won_at","") or "", "won_pipeline": d.get("won_pipeline","")}
             for d in deals if d.get("is_won")],
            key=lambda x: x["won_at"], reverse=True,
        )[:25],
        "demo_deals": [
            {"company": d.get("company_name",""), "researcher": d.get("researcher",""),
             "sdr": d.get("sdr",""), "stage": d.get("stage","")}
            for d in deals if d.get("is_demo_booked")
        ][:20],
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source": "NocoDB (synced from HubSpot)",
    }

# ── Data: Researcher activity (Google Sheets) ─────────────────────────────────

@_cached("researcher_activity", ttl=1800)
def _fetch_researcher_activity():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "2026 SDR ", max_rows=600)
    except Exception as e:
        return {"error": str(e), "rows": []}

    RESEARCHERS = {"chinju","veera","mohanapriya","tamil"}

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""
    def n(row, i):
        try: return int(row[i].strip()) if len(row) > i and row[i].strip() else 0
        except: return 0

    by_researcher = defaultdict(lambda: {"leads":0,"dials":0,"emails":0,"connects":0,"demos":0,"days":0})
    dates_seen = []
    for row in rows[2:]:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in RESEARCHERS: continue
        date = v(row,0)
        if date not in dates_seen: dates_seen.append(date)
        r = by_researcher[v(row,1)]
        r["leads"] += n(row,2); r["dials"] += n(row,4); r["emails"] += n(row,5)
        r["connects"] += n(row,6); r["demos"] += n(row,8); r["days"] += 1

    totals = []
    for name, stats in by_researcher.items():
        totals.append({
            "name": name, "leads": stats["leads"], "dials": stats["dials"],
            "emails": stats["emails"], "connects": stats["connects"],
            "demos": stats["demos"], "days": stats["days"],
            "connect_rate": round(stats["connects"]/stats["dials"]*100) if stats["dials"] else 0,
            "demo_rate":    round(stats["demos"]/stats["connects"]*100) if stats["connects"] else 0,
        })

    recent = []
    for row in rows[2:]:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        if v(row,1).lower() not in RESEARCHERS: continue
        if v(row,0) in dates_seen[-5:]:
            recent.append({"date":v(row,0),"rep":v(row,1),"leads":n(row,2),
                           "dials":n(row,4),"emails":n(row,5),"connects":n(row,6),"demos":n(row,8)})

    return {
        "totals":     totals,
        "recent":     recent,
        "date_range": f"{dates_seen[0] if dates_seen else '—'} – {dates_seen[-1] if dates_seen else '—'}",
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

# ── Data: SDR Review (NocoDB for pipeline, Sheets for call activity) ──────────

@_cached("sdr_activity", ttl=1800)
def _fetch_sdr_activity():
    # Stage counts from NocoDB (fast, no HubSpot needed)
    deals = _noco_get_all(NOCO_DEALS_TABLE, fields="sdr,stage_id,is_demo_booked,is_no_mobile,is_disqualified")

    SDR_PIPELINE_LABELS = {"Tim": "Tim's SDR Pipeline", "Jay": "Jay's SDR Pipeline"}
    hs_sdr = {}
    for sdr_name in ["Tim", "Jay"]:
        sdr_deals = [d for d in deals if d.get("sdr") == sdr_name]
        total = len(sdr_deals)
        not_contacted = sum(1 for d in sdr_deals if d.get("stage_id") in ("1276084786","1338730822"))
        called        = sum(1 for d in sdr_deals if d.get("stage_id") in ("1276084789","1338730823"))
        no_mobile     = sum(1 for d in sdr_deals if d.get("is_no_mobile"))
        unable        = sum(1 for d in sdr_deals if d.get("stage_id") in ("1321576882","1338730825"))
        demo          = sum(1 for d in sdr_deals if d.get("is_demo_booked"))
        follow_up     = sum(1 for d in sdr_deals if d.get("stage_id") in ("1276084791","1338730827"))
        disq          = sum(1 for d in sdr_deals if d.get("is_disqualified"))
        active_total  = total - disq
        hs_sdr[sdr_name] = {
            "total": total, "not_contacted": not_contacted, "called": called,
            "no_mobile": no_mobile, "unable_to_reach": unable,
            "demo_booked": demo, "future_followup": follow_up,
            "disqualified": disq,
            "demo_rate_pct":      round(demo/(called+unable+follow_up+demo)*100) if (called+unable+follow_up+demo) else 0,
            "phone_quality_pct":  round((active_total-no_mobile)/active_total*100) if active_total else 0,
        }

    # Call activity from Google Sheets
    kyle_rows = []
    try:
        kyle_rows = _read_sheet_tab(SALES_SHEET_ID, "Kyle Tracking 2026 SDR ", max_rows=300)
    except Exception:
        pass

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""
    def n(row, i):
        try: return int(row[i].strip()) if len(row) > i and row[i].strip() else 0
        except: return 0

    SDR_NAMES = {"tim","jay","natasha","tim h","tim huynh"}
    kyle_by_sdr = defaultdict(lambda: {"dials":0,"connects":0,"demos":0,"days":0})
    recent_kyle = []
    dates_seen = []
    for row in kyle_rows[2:]:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in SDR_NAMES: continue
        date = v(row,0)
        if date not in dates_seen: dates_seen.append(date)
        r = kyle_by_sdr[v(row,1)]
        r["dials"] += n(row,2); r["connects"] += n(row,3); r["demos"] += n(row,4); r["days"] += 1
        if date in dates_seen[-7:]:
            recent_kyle.append({"date":date,"rep":v(row,1),
                                 "dials":n(row,2),"connects":n(row,3),"demos":n(row,4)})

    sdr_totals = []
    for sdr, stats in kyle_by_sdr.items():
        sdr_totals.append({
            "name": sdr, "dials": stats["dials"], "connects": stats["connects"],
            "demos": stats["demos"], "days": stats["days"],
            "connect_rate": round(stats["connects"]/stats["dials"]*100) if stats["dials"] else 0,
            "demo_rate":    round(stats["demos"]/stats["connects"]*100) if stats["connects"] else 0,
        })

    return {
        "hs_pipeline":    hs_sdr,
        "call_activity":  sdr_totals,
        "recent_activity": recent_kyle[-40:],
        "updated_at":     datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source":    "NocoDB pipeline + Google Sheets call log",
    }

# ── Data: AE / Sales Review (HubSpot — Natasha + Main, small dataset) ─────────

@_cached("sales_ae", ttl=1800)
def _fetch_sales_ae():
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    cutoff_ms = str(int(cutoff.timestamp() * 1000))

    deals = {"active": [], "won": [], "lost": []}
    for pip_id, pip_label in [(PIPELINE_NATASHA,"Natasha"), (PIPELINE_MAIN,"Main Sales")]:
        after = None
        while True:
            body = {
                "filterGroups": [{"filters": [
                    {"propertyName":"pipeline","operator":"EQ","value":pip_id},
                    {"propertyName":"hs_lastmodifieddate","operator":"GTE","value":cutoff_ms},
                ]}],
                "properties": ["dealname","dealstage","closedate","createdate","amount"],
                "sorts": [{"propertyName":"createdate","direction":"DESCENDING"}],
                "limit": 100,
            }
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            for obj in resp.get("results", []):
                p = obj["properties"]
                sid = p.get("dealstage","")
                entry = {
                    "deal_id":   obj["id"],
                    "name":      p.get("dealname",""),
                    "stage":     NATASHA_STAGES.get(sid, sid),
                    "stage_id":  sid,
                    "pipeline":  pip_label,
                    "close_date":(p.get("closedate","") or "")[:10],
                    "created":   (p.get("createdate","") or "")[:10],
                    "amount":    p.get("amount",""),
                }
                if sid in WON_STAGE_IDS:   deals["won"].append(entry)
                elif sid in LOST_STAGE_IDS: deals["lost"].append(entry)
                else:                       deals["active"].append(entry)
            paging = resp.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after: break

    stage_order = ["Qualification Req","Demo Scheduled","Demo Attended","Closeable/WIP","Proposal Sent"]
    stage_counts = Counter(d["stage"] for d in deals["active"])
    funnel = [{"stage": s, "count": stage_counts.get(s,0)} for s in stage_order]

    return {
        "active":  deals["active"][:50],
        "won":     sorted(deals["won"],  key=lambda x: x["close_date"], reverse=True)[:30],
        "lost":    sorted(deals["lost"], key=lambda x: x["close_date"], reverse=True)[:20],
        "funnel":  funnel,
        "summary": {
            "active_count": len(deals["active"]),
            "won_count":    len(deals["won"]),
            "lost_count":   len(deals["lost"]),
            "won_30d":      sum(1 for d in deals["won"]
                               if d["close_date"] >= (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")),
        },
        "updated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source": "HubSpot (Natasha + Main Sales, 6-month delta)",
    }

# ── Data: Onboarding (Google Sheets) ─────────────────────────────────────────

@_cached("onboarding", ttl=1800)
def _fetch_onboarding():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "Onboarding Pipeline", max_rows=200)
    except Exception as e:
        return {"error": str(e), "customers": []}

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""

    headers = rows[0] if rows else []
    customers = []
    for row in rows[1:]:
        if len(row) < 2 or not v(row, 0): continue
        customers.append({col: v(row, i) for i, col in enumerate(headers) if col})

    phase_counts = Counter(c.get("Phase","") or c.get("Status","") or "Unknown" for c in customers)
    at_risk = [c for c in customers if any(
        x in (c.get("Status","") + c.get("Phase","")).lower()
        for x in ["risk","overdue","stuck","delayed","⚠","🔴"]
    )]

    return {
        "customers":    customers[:40],
        "total":        len(customers),
        "phase_counts": dict(phase_counts),
        "at_risk":      at_risk,
        "updated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

# ── API routes ────────────────────────────────────────────────────────────────

def _require_key():
    if not DASHBOARD_SECRET: return
    key = request.args.get("key") or request.headers.get("X-Dashboard-Key","")
    if key != DASHBOARD_SECRET:
        abort(403)

@app.route("/api/pipeline-quality")
def api_pipeline_quality():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("pipeline_quality")
    try:
        return jsonify({"ok": True, "data": _fetch_pipeline_quality()})
    except Exception as e:
        log.error(f"pipeline-quality error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/researchers")
def api_researchers():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("researcher_activity")
    try:
        return jsonify({"ok": True, "data": _fetch_researcher_activity()})
    except Exception as e:
        log.error(f"researchers error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/sdr")
def api_sdr():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("sdr_activity")
    try:
        return jsonify({"ok": True, "data": _fetch_sdr_activity()})
    except Exception as e:
        log.error(f"sdr error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/sales")
def api_sales():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("sales_ae")
    try:
        return jsonify({"ok": True, "data": _fetch_sales_ae()})
    except Exception as e:
        log.error(f"sales error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/onboarding")
def api_onboarding():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("onboarding")
    try:
        return jsonify({"ok": True, "data": _fetch_onboarding()})
    except Exception as e:
        log.error(f"onboarding error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "version": "2.0.0",
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "data_architecture": "NocoDB + Sheets (HubSpot only for AE pipeline)"})

@app.route("/")
def index():
    return render_template("index.html", dashboard_secret=DASHBOARD_SECRET, version="2.0.0")

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))
