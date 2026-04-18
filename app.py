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
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, render_template, abort
from dotenv import load_dotenv

load_dotenv()

APP_VERSION = "v2.10.0"

app = Flask(__name__)
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ── Config ────────────────────────────────────────────────────────────────────

HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY", "")
SALES_SHEET_ID    = os.getenv("SALES_SHEET_ID", "1qN420Yk7RFXUFLfmT9xecWGBYuM0TEhBSvBgi2kH_ng")
DASHBOARD_SECRET  = os.getenv("DASHBOARD_SECRET", "")
PAUL_SECRET       = os.getenv("PAUL_SECRET", "")   # gates owner-only pages (Candidate Audit)
CACHE_TTL_SECS    = int(os.getenv("CACHE_TTL", "1800"))
GOOGLE_SA_B64     = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SA_PATH    = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "")
NOCO_BASE_URL       = os.getenv("NOCODB_BASE_URL", "https://app.nocodb.com")
NOCO_TOKEN          = os.getenv("NOCODB_TOKEN", "")
NOCO_PROJECT_ID     = os.getenv("NOCODB_PROJECT_ID", "pmnqbv38yudws3v")
NOCO_DEALS_TABLE    = os.getenv("NOCODB_DEALS_TABLE_ID", "migi5lc0fb9zhie")
NOCO_SDR_CALLS_TBL  = os.getenv("NOCODB_SDR_CALLS_TBL",   "mrhbubrruqt6mxi")
NOCO_SDR_DAILY_AGG  = os.getenv("NOCODB_SDR_DAILY_AGG",   "mb6u0m9d0q0y08q")
# Candidate audit — Employee Nirvana base
NOCO_HR_BASE_ID     = os.getenv("NOCODB_HR_BASE_ID",    "ptha3v85jd54lva")
NOCO_CANDIDATES_TBL = os.getenv("NOCODB_CANDIDATES_TBL","mhgfkpd0xwc51ca")

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
    """Fetch ALL rows from a NocoDB table (handles pagination + 429 retry)."""
    rows = []
    page = 1
    PAGE_SIZE = 100
    MAX_RETRIES = 5
    while True:
        params = {"limit": PAGE_SIZE, "offset": (page - 1) * PAGE_SIZE}
        if where:  params["where"]  = where
        if fields: params["fields"] = fields
        for attempt in range(MAX_RETRIES):
            r = requests.get(
                f"{NOCO_BASE_URL}/api/v1/db/data/noco/{NOCO_PROJECT_ID}/{table_id}",
                headers=_noco_headers(), params=params,
            )
            if r.status_code == 429:
                wait = 2.0 * (2 ** attempt)
                log.warning(f"NocoDB 429 on page {page}, retrying in {wait:.1f}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            break
        data = r.json()
        batch = data.get("list", [])
        rows.extend(batch)
        page_info = data.get("pageInfo", {})
        if page_info.get("isLastPage", True) or len(batch) < PAGE_SIZE:
            break
        page += 1
        time.sleep(0.4)   # gentle pacing between pages
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

# ── Rule-based ICP scorer (no Claude required) ────────────────────────────────
# Used for deals that haven't been scored by the Alfred batch scorer yet.
# Tier A = strong trade/FSM signal, B = moderate, C = unclear, X = disqualified.

_ICP_KEYWORDS_A = [
    "air con","aircon","hvac","refriger","cool room","coldroom","cold room","chiller",
    "mechanical services","mechanical service","fire protect","fire sprinkl",
    "plumb","gas fitt","building services","building service","gas & plumb",
    "air conditioning","a/c contractor","pest control services",
    "electrical contractor","electrical services","electrical engineer",
]
_ICP_KEYWORDS_B = [
    "pest control","facilities manage","property mainten","mainten service",
    "trade service","field service","service tech","service group","technician",
    "install","service & mainten","asset manag","fleet mainten",
]
_ICP_DISQUALIFY = [
    "software","saas","digital agency","marketing agency","media agency",
    "restaurant","cafe","hotel","motel","hospitality","retail store","shop pty",
    "finance","insurance broker","law firm","solicitor","solicitors",
    "school ","university","college","real estate agency","property group",
    "consulting group","management consult",
]
# HubSpot industry values that map to trade
_ICP_INDUSTRY_A = {
    "construction","electrical_electronic_manufacturing",
    "mechanical_or_industrial_engineering","utilities",
}
_ICP_INDUSTRY_B = {
    "facilities_services","environmental_services","consumer_services",
    "industrial_automation","real_estate",
}


def _rule_icp_tier(company_name: str, industry: str = "") -> str:
    """
    Keyword + industry-based ICP tier for companies without a Claude score.
    Returns "A", "B", "C", or "X".
    """
    name = (company_name or "").lower()
    ind  = (industry or "").lower().replace(" ", "_")

    for kw in _ICP_DISQUALIFY:
        if kw in name:
            return "X"

    for kw in _ICP_KEYWORDS_A:
        if kw in name:
            return "A"
    if ind in _ICP_INDUSTRY_A:
        return "A"

    for kw in _ICP_KEYWORDS_B:
        if kw in name:
            return "B"
    if ind in _ICP_INDUSTRY_B:
        return "B"

    return "C"


def _effective_icp_tier(deal: dict) -> str:
    """
    Return the best available ICP tier for a deal.
    Prefers Claude-scored tier (from NocoDB icp_tier), falls back to rule-based.
    """
    raw = (deal.get("icp_tier") or "").strip()
    # Accept only clean tier values — reject rule-prefixed strings
    if raw in ("A", "B", "C", "X"):
        return raw
    return _rule_icp_tier(
        deal.get("company_name", ""),
        deal.get("industry", ""),
    )


def _icp_score_note(deal: dict) -> str:
    """
    Return a human-readable explanation of how the ICP tier was determined.
    Uses icp_score if it contains a reason (set by rule_score_pipeline.py or
    Alfred's Claude scorer). Falls back to the rule engine with explanation.
    """
    raw_score = (deal.get("icp_score") or "").strip()
    # Alfred Claude scorer writes numeric scores; rule scorer writes "rule:X | reason"
    if raw_score.startswith("rule:") and "|" in raw_score:
        return raw_score.split("|", 1)[1].strip()
    if raw_score and not raw_score.startswith("rule:"):
        # Looks like a real Claude score — indicate it
        tier = (deal.get("icp_tier") or "").strip()
        return f"Claude-scored (score: {raw_score})" if raw_score else f"Claude-scored tier {tier}"
    # Generate a fresh rule explanation
    name = (deal.get("company_name") or "").lower()
    ind  = (deal.get("industry")      or "").lower().replace(" ", "_")
    for kw in _ICP_DISQUALIFY:
        if kw in name:
            return f"Disqualified — keyword: '{kw}'"
    for kw in _ICP_KEYWORDS_A:
        if kw in name:
            return f"Tier A — keyword: '{kw}' (HVAC / Electrical / Fire / Plumbing)"
    if ind in _ICP_INDUSTRY_A:
        return f"Tier A — industry: '{ind}'"
    for kw in _ICP_KEYWORDS_B:
        if kw in name:
            return f"Tier B — keyword: '{kw}' (Maintenance / Facility / Cleaning)"
    if ind in _ICP_INDUSTRY_B:
        return f"Tier B — industry: '{ind}'"
    if ind:
        return f"Tier C — no trade signal (industry on file: '{ind}')"
    return "Tier C — no trade keyword or industry signal found in company name"


@_cached("daily_hs_stats", ttl=1800)
def _fetch_daily_hs_stats() -> dict:
    """
    Aggregate pipeline_deals by deal_created date.
    Returns {
      'by_date':  {iso_date: {count, icp_a, icp_b, icp_c, icp_x, icp_ab, icp_pct}},
      'by_week':  {week_key: {week_label, week_start, week_end, count, icp_ab, icp_pct}},
    }
    One unique company per date (deduplicated by company_id/name within the same day).
    Researcher attribution is not available in HubSpot — these are day-level totals.
    """
    from datetime import date as date_type

    deals = _noco_get_all(
        NOCO_DEALS_TABLE,
        fields="company_id,company_name,industry,icp_tier,deal_created",
    )

    # Group by date — deduplicate companies added on same day
    by_date: dict = {}
    for d in deals:
        dc = (d.get("deal_created") or "").strip()[:10]
        if not dc:
            continue
        cid  = str(d.get("company_id") or "").strip()
        name = (d.get("company_name") or "").strip().lower()
        key  = cid if cid else name
        if not key:
            continue
        if dc not in by_date:
            by_date[dc] = {}
        if key not in by_date[dc]:
            by_date[dc][key] = d   # keep first occurrence per company per day

    # Compute ICP counts per day
    result_by_date: dict = {}
    for dc, companies in by_date.items():
        counts = {"count": 0, "icp_a": 0, "icp_b": 0, "icp_c": 0, "icp_x": 0}
        for deal in companies.values():
            tier = _effective_icp_tier(deal)
            counts["count"] += 1
            counts[f"icp_{tier.lower()}"] = counts.get(f"icp_{tier.lower()}", 0) + 1
        ab  = counts["icp_a"] + counts["icp_b"]
        pct = round(ab / counts["count"] * 100) if counts["count"] else 0
        result_by_date[dc] = {**counts, "icp_ab": ab, "icp_pct": pct}

    # Roll up to ISO weeks
    by_week: dict = {}
    for dc, stats in result_by_date.items():
        try:
            dt   = datetime.strptime(dc, "%Y-%m-%d").date()
            iso  = dt.isocalendar()
            wkey = f"{iso[0]}-W{iso[1]:02d}"
            # Week Mon–Sun boundaries
            mon = dt - timedelta(days=dt.weekday())
            sun = mon + timedelta(days=6)
            if wkey not in by_week:
                by_week[wkey] = {
                    "week_key":   wkey,
                    "week_label": f"{mon.strftime('%-d %b')}–{sun.strftime('%-d %b')}",
                    "week_start": mon.isoformat(),
                    "week_end":   sun.isoformat(),
                    "count": 0, "icp_a": 0, "icp_b": 0, "icp_c": 0, "icp_x": 0,
                    "icp_ab": 0,
                }
            for k in ("count","icp_a","icp_b","icp_c","icp_x","icp_ab"):
                by_week[wkey][k] += stats.get(k, 0)
        except Exception:
            continue

    for w in by_week.values():
        w["icp_pct"] = round(w["icp_ab"] / w["count"] * 100) if w["count"] else 0

    return {"by_date": result_by_date, "by_week": by_week}


# ── Data: Mark Lang (Jay) HubSpot call stats by date ─────────────────────────
MARK_LANG_OWNER_ID = "104392067"
BOOKED_KEYWORDS = ["booked","booking","demo","scheduled","meeting","next week","next tuesday",
                   "next wednesday","next thursday","next friday","april","may"]
CONNECTED_KEYWORDS = ["spoke to","spoke with","booked","interested","call back",
                      "sent","email","talked","confirmed","agreed","he said","she said",
                      "not interested","wrong number","busy","annual leave"]
NO_ANSWER_KEYWORDS = ["no answer","no ans","n/a","didn't answer","not answering","voicemail","vm"]

@_cached("mark_calls_by_date", ttl=1800)
def _fetch_mark_calls_by_date() -> dict:
    """
    Fetches all outbound calls from Mark Lang (owner 104392067) from HubSpot.
    Returns { iso_date: { dials, connected, no_answer, demos_booked } }
    """
    import re as _re
    hs_key = os.environ.get("HUBSPOT_API_KEY", "")
    if not hs_key:
        return {}

    headers_hs = {"Authorization": f"Bearer {hs_key}"}
    since_ms = str(int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp() * 1000))

    by_date: dict = {}
    after = None

    while True:
        payload = {
            "properties": ["hs_call_body", "hs_timestamp", "hs_call_direction", "hs_call_source"],
            "filterGroups": [{"filters": [
                {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": MARK_LANG_OWNER_ID},
                {"propertyName": "hs_call_direction", "operator": "EQ", "value": "OUTBOUND"},
                {"propertyName": "hs_timestamp", "operator": "GT", "value": since_ms},
            ]}],
            "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            "limit": 100,
        }
        if after:
            payload["after"] = after

        try:
            r = requests.post(
                "https://api.hubapi.com/crm/v3/objects/calls/search",
                headers={**headers_hs, "Content-Type": "application/json"},
                json=payload,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"Mark calls fetch error: {e}")
            break

        for res in data.get("results", []):
            p = res["properties"]
            ts = (p.get("hs_timestamp") or "")[:10]
            if not ts:
                continue
            body = _re.sub(r"<[^>]+>", "", (p.get("hs_call_body") or "")).strip().lower()
            if ts not in by_date:
                by_date[ts] = {"dials": 0, "connected": 0, "no_answer": 0, "demos_booked": 0}
            d = by_date[ts]
            d["dials"] += 1
            if any(k in body for k in NO_ANSWER_KEYWORDS):
                d["no_answer"] += 1
            elif any(k in body for k in CONNECTED_KEYWORDS):
                d["connected"] += 1
            if any(k in body for k in BOOKED_KEYWORDS):
                d["demos_booked"] += 1

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return by_date


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
        icp_ab = sum(1 for d in rdl if _effective_icp_tier(d) in ("A","B"))
        phone_ok = active - no_mob
        researcher_stats.append({
            "name":      researcher,
            "total":     total,
            "active":    active,
            "no_mobile": no_mob,
            "phone_ok":  phone_ok,
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

    # ICP industry breakdown for the drill-down click
    icp_ind     = Counter()
    non_icp_ind = Counter()
    for d in deals:
        tier = _effective_icp_tier(d)
        ind  = (d.get("industry") or "").strip() or "Unknown"
        ind_label = ind.replace("_", " ").title() if ind != "Unknown" else "Unknown / Not Set"
        if tier in ("A", "B"):
            icp_ind[ind_label] += 1
        else:
            non_icp_ind[ind_label] += 1


    # ── Ops Health: 4-pillar summary ────────────────────────────────────────
    icp_ab_cnt  = sum(1 for d in deals if _effective_icp_tier(d) in ("A","B"))
    demo_booked = sum(1 for d in deals if d.get("is_demo_booked"))
    won         = sum(1 for d in deals if d.get("is_won"))
    no_mobile   = sum(1 for d in deals if d.get("is_no_mobile"))
    try:
        agg = _fetch_sdr_daily_agg()
        total_dials = sum(int(r.get("dials",0) or 0) for r in agg)
        total_conn  = sum(int(r.get("connects",0) or 0) for r in agg)
        sdr_connect_pct = round(total_conn/total_dials*100) if total_dials else 0
        total_intro = sum(int(r.get("said_intro",0) or 0) for r in agg)
        sdr_funnel_pct = round(total_intro/total_conn*100) if total_conn else 0
    except Exception:
        sdr_connect_pct, sdr_funnel_pct = 0, 0
        total_conn = total_dials = 0
    try:
        onb = _fetch_onboarding()
        onb_active  = onb.get("summary",{}).get("active", 0)
        onb_at_risk = onb.get("summary",{}).get("at_risk", 0)
        onb_health_pct = round((onb_active-onb_at_risk)/onb_active*100) if onb_active else 0
    except Exception:
        onb_health_pct, onb_active, onb_at_risk = 0, 0, 0
    def _band(v, good, warn):
        if v >= good: return "good"
        if v >= warn: return "warn"
        return "bad"
    research_icp_pct = round(icp_ab_cnt/total*100) if total else 0
    ae_demo_to_won   = round(won/demo_booked*100) if demo_booked else 0
    ops_health = {
        "research": {
            "label":"Research Quality","metric":"ICP A+B of researched leads",
            "value":f"{research_icp_pct}%","score":research_icp_pct,
            "detail":f"{icp_ab_cnt} of {total} total deals",
            "band":_band(research_icp_pct, 60, 40),
            "sub":f"{no_mobile} ({round(no_mobile/total*100) if total else 0}%) missing phone",
        },
        "sdr": {
            "label":"SDR Quality","metric":"Connect rate · funnel progression",
            "value":f"{sdr_connect_pct}%","score":sdr_connect_pct,
            "detail":f"{total_conn:,} connects of {total_dials:,} dials",
            "band":_band(sdr_connect_pct, 35, 20),
            "sub":f"{sdr_funnel_pct}% of connects reached intro stage",
        },
        "ae": {
            "label":"AE Conversion","metric":"Demo → Won",
            "value":f"{ae_demo_to_won}%","score":ae_demo_to_won,
            "detail":f"{won} wins from {demo_booked} demos booked",
            "band":_band(ae_demo_to_won, 25, 10),
            "sub":f"{active} active deals in pipeline",
        },
        "onboarding": {
            "label":"Onboarding Health","metric":"Customers on track",
            "value":f"{onb_health_pct}%","score":onb_health_pct,
            "detail":f"{onb_active - onb_at_risk} of {onb_active} active",
            "band":_band(onb_health_pct, 85, 65),
            "sub":(f"{onb_at_risk} at-risk" if onb_at_risk else "No at-risk accounts"),
        },
    }

    return {
        "summary": {
            "total":         total,
            "active":        active,
            "disqualified":  total - active,
            "demo_booked":   sum(1 for d in deals if d.get("is_demo_booked")),
            "no_mobile":     sum(1 for d in deals if d.get("is_no_mobile")),
            "won":           sum(1 for d in deals if d.get("is_won")),
            "icp_ab":        sum(1 for d in deals if _effective_icp_tier(d) in ("A","B")),
            "no_mobile_pct": round(sum(1 for d in deals if d.get("is_no_mobile"))/total*100) if total else 0,
            "last_hs_sync":  last_sync,
        },
        "ops_health":    ops_health,
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
        "icp_industry":     dict(icp_ind.most_common(12)),
        "non_icp_industry": dict(non_icp_ind.most_common(12)),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source": "NocoDB (synced from HubSpot)",
    }

# ── Date helpers ─────────────────────────────────────────────────────────────

def _parse_sheet_date(raw: str):
    """
    Try to parse a date string from a Google Sheet into a datetime.date.
    Handles common formats: 'Mon, Apr 12, 2026' / '12/04/2026' / '2026-04-12' / '12 Apr 2026'
    Returns datetime.date or None if unparseable.
    """
    from datetime import date as _date
    import re
    s = raw.strip()
    if not s:
        return None
    for fmt in (
        "%a, %b %d, %Y",   # Mon, Apr 12, 2026
        "%A, %B %d, %Y",   # Monday, April 12, 2026
        "%a %b %d %Y",     # Mon Apr 12 2026 (no comma)
        "%d/%m/%Y",         # 12/04/2026  (AU default)
        "%m/%d/%Y",         # 04/12/2026  (US)
        "%Y-%m-%d",         # 2026-04-12  (ISO)
        "%d %b %Y",         # 12 Apr 2026
        "%d %B %Y",         # 12 April 2026
        "%b %d, %Y",        # Apr 12, 2026
        "%B %d, %Y",        # April 12, 2026
        "%d-%b-%Y",         # 12-Apr-2026
        "%d-%b-%y",         # 12-Apr-26
        "%d/%m/%y",         # 12/04/26
        "%m/%d/%y",         # 04/12/26
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def _recent_date_cutoff(days: int = 14):
    """Return a date N calendar days ago (for filtering sheet rows)."""
    return (datetime.now(timezone.utc) - timedelta(days=days)).date()


# ── Data: Researcher activity (Google Sheets) ─────────────────────────────────

@_cached("researcher_activity", ttl=1800)
def _fetch_researcher_activity():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "2026 SDR ", max_rows=600)
    except Exception as e:
        return {"error": str(e), "rows": []}

    RESEARCHERS = {"chinju","veera","mohanapriya","tamil"}
    cutoff = _recent_date_cutoff(days=14)   # look back 14 calendar days max
    today  = datetime.now(timezone.utc).date()

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""
    def n(row, i):
        try: return int(row[i].strip()) if len(row) > i and row[i].strip() else 0
        except: return 0

    by_researcher = defaultdict(lambda: {"leads":0,"dials":0,"emails":0,"connects":0,"demos":0,"days":0})
    all_dates = []
    recent_rows = []  # (parsed_date, raw_date_str, row)

    for row in rows[2:]:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in RESEARCHERS: continue

        raw_date = v(row,0)
        parsed   = _parse_sheet_date(raw_date)

        # Accumulate all-time totals
        r = by_researcher[v(row,1)]
        r["leads"] += n(row,2); r["dials"] += n(row,4); r["emails"] += n(row,5)
        r["connects"] += n(row,6); r["demos"] += n(row,8); r["days"] += 1

        if raw_date not in all_dates:
            all_dates.append(raw_date)

        # Only include in recent if date is parseable AND within last 14 days AND not future
        if parsed and cutoff <= parsed <= today:
            recent_rows.append((parsed, raw_date, row))

    # If no parseable dates fell within 14 days, fall back to last 7 sheet rows by date order
    if not recent_rows:
        fallback_dates = all_dates[-7:]
        for row in rows[2:]:
            if len(row) < 2 or not v(row,0) or not v(row,1): continue
            if v(row,1).lower() not in RESEARCHERS: continue
            if v(row,0) in fallback_dates:
                recent_rows.append((None, v(row,0), row))

    # Sort recent by parsed date descending (most recent first)
    recent_rows.sort(key=lambda x: x[0] or datetime.min.date(), reverse=True)

    recent = []
    seen_dates = []
    for parsed, raw_date, row in recent_rows:
        if raw_date not in seen_dates: seen_dates.append(raw_date)
        if len(seen_dates) > 7: break   # cap at 7 distinct days
        recent.append({
            "date":     raw_date,
            "rep":      v(row,1),
            "leads":    n(row,2),
            "dials":    n(row,4),
            "emails":   n(row,5),
            "connects": n(row,6),
            "demos":    n(row,8),
        })

    # Merge NocoDB pipeline stats (phone% + ICP%) per researcher
    try:
        pq = _fetch_pipeline_quality()
        noco_by_lower = {r["name"].lower(): r for r in pq.get("by_researcher", [])}
    except Exception as e:
        log.warning(f"Could not fetch pipeline quality for researcher merge: {e}")
        noco_by_lower = {}

    totals = []
    for name, stats in by_researcher.items():
        noco = noco_by_lower.get(name.lower(), {})
        totals.append({
            "name":        name,
            "leads":       stats["leads"],
            "days":        stats["days"],
            # Phone + ICP come from NocoDB pipeline data
            "phone_count": noco.get("phone_ok", 0),
            "phone_pct":   noco.get("phone_pct", 0),
            "icp_ab":      noco.get("icp_ab", 0),
            "icp_pct":     noco.get("icp_pct", 0),
            "noco_total":  noco.get("total", 0),
        })

    date_range_start = all_dates[0]  if all_dates else "—"
    date_range_end   = all_dates[-1] if all_dates else "—"

    # Fetch HubSpot daily stats (ICP quality per day, day-level totals)
    try:
        hs_stats    = _fetch_daily_hs_stats()
        hs_by_date  = hs_stats.get("by_date", {})
        hs_by_week  = hs_stats.get("by_week",  {})
    except Exception as e:
        log.warning(f"Could not fetch daily HS stats: {e}")
        hs_by_date = {}
        hs_by_week = {}

    # Fetch Mark Lang (Jay) HubSpot call stats by date
    try:
        mark_calls = _fetch_mark_calls_by_date()
    except Exception as e:
        log.warning(f"Could not fetch Mark call stats: {e}")
        mark_calls = {}

    # All individual rows with ISO dates — used by frontend date-filter controls
    all_rows_export = []
    for row in rows[2:]:
        if len(row) < 2 or not v(row, 0) or not v(row, 1): continue
        rep = v(row, 1).lower()
        if rep not in RESEARCHERS: continue
        raw_date = v(row, 0)
        parsed   = _parse_sheet_date(raw_date)
        iso      = parsed.isoformat() if parsed else None
        hs_day   = hs_by_date.get(iso, {}) if iso else {}
        mark_day = mark_calls.get(iso, {})   if iso else {}
        all_rows_export.append({
            "date":       raw_date,
            "iso_date":   iso,
            "rep":        v(row, 1),
            "leads":      n(row, 2),
            "dials":      n(row, 4),
            "emails":     n(row, 5),
            "connects":   n(row, 6),
            "demos":      n(row, 8),
            # HubSpot pipeline quality for this calendar day (all researchers combined)
            "hs_leads":   hs_day.get("count"),
            "hs_icp_ab":  hs_day.get("icp_ab"),
            "hs_icp_pct": hs_day.get("icp_pct"),
            # Mark Lang (Jay) HubSpot call stats for this day
            "mark_dials":     mark_day.get("dials"),
            "mark_connected": mark_day.get("connected"),
            "mark_demos":     mark_day.get("demos_booked"),
        })

    # Build weekly rollup from hs_by_week — attach ISO week key to each sheet row
    # so the frontend can group rows into weeks and show week-level quality
    for row_export in all_rows_export:
        if row_export.get("iso_date"):
            try:
                dt   = datetime.strptime(row_export["iso_date"], "%Y-%m-%d")
                iso  = dt.isocalendar()
                row_export["week_key"] = f"{iso[0]}-W{iso[1]:02d}"
            except Exception:
                pass

    # Weekly summaries sorted oldest → newest
    weekly_summaries = sorted(hs_by_week.values(), key=lambda w: w["week_start"])

    parsed_count = sum(1 for r in all_rows_export if r.get("iso_date"))
    log.info(f"Researcher activity: {len(all_rows_export)} rows, {parsed_count} with parsed dates, "
             f"sample date = {all_rows_export[0]['date'] if all_rows_export else 'none'}")

    return {
        "totals":          totals,
        "recent":          recent,
        "all_rows":        all_rows_export,
        "weekly":          weekly_summaries,
        "date_range":      f"{date_range_start} – {date_range_end}",
        "parsed_dates":    parsed_count,
        "total_rows":      len(all_rows_export),
        "updated_at":      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

# ── Data: SDR calls from NocoDB (HubSpot-sourced, pre-aggregated) ────────────

@_cached("sdr_daily_agg", ttl=1800)
def _fetch_sdr_daily_agg() -> list:
    """Fetch the pre-aggregated daily summary (~128 rows) — fast & cheap.
    Populated by `alfred/tools/sync_sdr_calls.py rebuild_daily_agg()`."""
    return _noco_get_all(
        NOCO_SDR_DAILY_AGG,
        fields=("iso_date,sdr,week_key,dials,connects,said_intro,had_convo,"
                "asked_meeting,booked_meeting,unique_companies,"
                "icp_a,icp_b,icp_c,icp_x,updated_at,drill_json"),
    )


@_cached("sdr_calls_by_sdr", ttl=1800)
def _fetch_sdr_calls_by_sdr(sdr_filter: str) -> list:
    """Fetch all calls for one SDR in parallel page-chunks.
    NocoDB can't filter on call_date — we pull by sdr, filter dates in-memory.
    Cached 30min; ~5k rows max per SDR."""
    where = f"(sdr,eq,{sdr_filter.capitalize()})" if sdr_filter.lower() != "all" else None
    fields = ("call_id,sdr,call_date,call_timestamp,outcome,duration_ms,"
              "said_intro,had_convo,asked_meeting,booked_meeting,"
              "contact_name,contact_phone,contact_mobile,"
              "company_id,company_name,company_domain,company_industry,"
              "icp_tier,icp_score_note,icp_source")

    PAGE_SIZE = 100
    MAX_RETRIES = 4
    url = f"{NOCO_BASE_URL}/api/v1/db/data/noco/{NOCO_PROJECT_ID}/{NOCO_SDR_CALLS_TBL}"

    def _get_page(offset: int) -> tuple:
        params = {"limit": PAGE_SIZE, "offset": offset, "fields": fields}
        if where: params["where"] = where
        for attempt in range(MAX_RETRIES):
            r = requests.get(url, headers=_noco_headers(), params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(1.5 * (2 ** attempt))
                continue
            r.raise_for_status()
            j = r.json()
            return j.get("list", []), j.get("pageInfo", {}).get("isLastPage", True)
        return [], True

    first_batch, is_last = _get_page(0)
    rows = list(first_batch)
    if is_last or len(first_batch) < PAGE_SIZE:
        log.info(f"NocoDB: loaded {len(rows)} rows from {NOCO_SDR_CALLS_TBL} (sdr={sdr_filter})")
        return rows

    offsets = list(range(PAGE_SIZE, 60 * PAGE_SIZE, PAGE_SIZE))
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_get_page, off): off for off in offsets}
        hit_last = False
        for fut in as_completed(futures):
            batch, last = fut.result()
            rows.extend(batch)
            if last or len(batch) < PAGE_SIZE:
                hit_last = True
        if not hit_last:
            log.warning("sdr_calls: hit 60-page parallel cap — may be truncated")
    log.info(f"NocoDB: loaded {len(rows)} rows from {NOCO_SDR_CALLS_TBL} (sdr={sdr_filter}, parallel)")
    return rows


def _fetch_sdr_calls_by_date(iso_date: str, sdr_filter: str = "all") -> list:
    """Return calls matching {iso_date, sdr_filter} — filters in Python."""
    rows = _fetch_sdr_calls_by_sdr(sdr_filter)
    return [r for r in rows if (r.get("call_date") or "")[:10] == iso_date]


def _aggregate_sdr_calls_by_date(agg_rows: list) -> dict:
    """
    Convert rows from `sdr_daily_agg` table into
      { iso_date: { sdr: { dials, connects, connect_pct, said_intro, ..., icp_ab_pct } } }
    """
    by_date: dict = {}
    for r in agg_rows:
        sdr = (r.get("sdr") or "").strip().title()
        iso = (r.get("iso_date") or "")[:10]
        if not sdr or not iso: continue
        dials    = int(r.get("dials") or 0)
        connects = int(r.get("connects") or 0)
        hs_conn  = int(r.get("hs_connects") or 0)
        a = int(r.get("icp_a") or 0)
        b = int(r.get("icp_b") or 0)
        c = int(r.get("icp_c") or 0)
        x = int(r.get("icp_x") or 0)
        total_co = int(r.get("unique_companies") or 0) or (a + b + c + x)
        ab = a + b
        by_date.setdefault(iso, {})[sdr] = {
            "dials":          dials,
            "connects":       connects,
            "hs_connects":    hs_conn,
            "connect_pct":    round(connects / dials * 100) if dials else 0,
            "hs_connect_pct": round(hs_conn / dials * 100) if dials else 0,
            "said_intro":     int(r.get("said_intro") or 0),
            "had_convo":      int(r.get("had_convo") or 0),
            "asked_meeting":  int(r.get("asked_meeting") or 0),
            "booked_meeting": int(r.get("booked_meeting") or 0),
            "unique_companies": total_co,
            "icp_a":     a, "icp_b": b, "icp_c": c, "icp_x": x,
            "icp_ab":    ab,
            "icp_ab_pct": round(ab / total_co * 100) if total_co else 0,
        }
    return by_date


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
    kyle_recent_rows = []  # (parsed_date, raw_date, row)
    cutoff = _recent_date_cutoff(days=14)
    today  = datetime.now(timezone.utc).date()

    for row in kyle_rows[2:]:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in SDR_NAMES: continue
        raw_date = v(row,0)
        parsed   = _parse_sheet_date(raw_date)
        r = kyle_by_sdr[v(row,1)]
        r["dials"] += n(row,2); r["connects"] += n(row,3); r["demos"] += n(row,4); r["days"] += 1
        if parsed and cutoff <= parsed <= today:
            kyle_recent_rows.append((parsed, raw_date, row))

    # Fallback: if no parseable dates in window, use last 7 unique sheet dates
    if not kyle_recent_rows:
        all_kyle_dates = []
        for row in kyle_rows[2:]:
            if len(row) < 2 or not v(row,0) or not v(row,1): continue
            if v(row,1).lower() not in SDR_NAMES: continue
            d = v(row,0)
            if d not in all_kyle_dates: all_kyle_dates.append(d)
        for row in kyle_rows[2:]:
            if len(row) < 2 or not v(row,0) or not v(row,1): continue
            if v(row,1).lower() not in SDR_NAMES: continue
            if v(row,0) in all_kyle_dates[-7:]:
                kyle_recent_rows.append((None, v(row,0), row))

    kyle_recent_rows.sort(key=lambda x: x[0] or datetime.min.date(), reverse=True)

    recent_kyle = []
    seen_dates  = []
    for parsed, raw_date, row in kyle_recent_rows:
        if raw_date not in seen_dates: seen_dates.append(raw_date)
        if len(seen_dates) > 7: break
        recent_kyle.append({"date": raw_date, "rep": v(row,1),
                             "dials": n(row,2), "connects": n(row,3), "demos": n(row,4)})

    sdr_totals = []
    for sdr, stats in kyle_by_sdr.items():
        sdr_totals.append({
            "name": sdr, "dials": stats["dials"], "connects": stats["connects"],
            "demos": stats["demos"], "days": stats["days"],
            "connect_rate": round(stats["connects"]/stats["dials"]*100) if stats["dials"] else 0,
            "demo_rate":    round(stats["demos"]/stats["connects"]*100) if stats["connects"] else 0,
        })

    # All individual rows with ISO dates — used by frontend date-filter controls
    all_kyle_rows_export = []
    for row in kyle_rows[2:]:
        if len(row) < 2 or not v(row, 0) or not v(row, 1): continue
        if v(row, 1).lower() not in SDR_NAMES: continue
        raw_date = v(row, 0)
        parsed   = _parse_sheet_date(raw_date)
        all_kyle_rows_export.append({
            "date":     raw_date,
            "iso_date": parsed.isoformat() if parsed else None,
            "rep":      v(row, 1),
            "dials":    n(row, 2),
            "connects": n(row, 3),
            "demos":    n(row, 4),
        })

    # ── HubSpot-sourced call data (from pre-aggregated summary table) ─────────
    try:
        agg_rows = _fetch_sdr_daily_agg()
    except Exception as e:
        log.warning(f"Could not fetch sdr_daily_agg: {e}")
        agg_rows = []

    hs_by_date = _aggregate_sdr_calls_by_date(agg_rows)

    # Flatten hs_by_date into all_hs_rows (one row per sdr per date)
    # Normalize sdr name to Title Case so HubSpot "Tim"/"Jay" matches Kyle's "TIM"/"JAY"
    all_hs_rows = []
    for iso, sdrs in hs_by_date.items():
        for sdr, s in sdrs.items():
            all_hs_rows.append({"iso_date": iso, "sdr": (sdr or "").title(), **s})
    all_hs_rows.sort(key=lambda r: (r["iso_date"], r["sdr"]), reverse=True)

    # Merged daily breakdown — one row per (sdr, date), combining HubSpot + sheet
    merged_rows = []
    # Normalize kyle rep names to Title Case for merging
    for kr in all_kyle_rows_export:
        kr["rep"] = (kr.get("rep") or "").title()

    kyle_lookup = {}
    for kr in all_kyle_rows_export:
        if kr["iso_date"]:
            key = (kr["rep"].lower(), kr["iso_date"])
            kyle_lookup[key] = kr

    # Seed merged set with all dates seen in either source
    seen = set()
    for hr in all_hs_rows:
        key = (hr["sdr"].lower(), hr["iso_date"])
        seen.add(key)
        kr = kyle_lookup.get(key, {})
        merged_rows.append({
            "iso_date":       hr["iso_date"],
            "sdr":            hr["sdr"],
            # HubSpot (primary)
            "dials_hs":       hr["dials"],
            "connects_hs":    hr["connects"],
            "connect_pct":    hr["connect_pct"],
            "said_intro":     hr["said_intro"],
            "had_convo":      hr["had_convo"],
            "asked_meeting":  hr["asked_meeting"],
            "booked_meeting": hr["booked_meeting"],
            "icp_a":          hr["icp_a"],
            "icp_b":          hr["icp_b"],
            "icp_c":          hr["icp_c"],
            "icp_x":          hr["icp_x"],
            "icp_ab":         hr["icp_ab"],
            "icp_ab_pct":     hr["icp_ab_pct"],
            "unique_companies": hr["unique_companies"],
            # Kyle sheet (cross-check)
            "dials_sheet":    kr.get("dials", 0),
            "connects_sheet": kr.get("connects", 0),
            "demos_sheet":    kr.get("demos", 0),
        })

    # Add kyle-only rows (days SDR reported activity but HubSpot had none)
    for key, kr in kyle_lookup.items():
        if key in seen: continue
        sdr_name = kr["rep"]
        merged_rows.append({
            "iso_date":       kr["iso_date"],
            "sdr":            sdr_name,
            "dials_hs":       0, "connects_hs": 0, "hs_connects": 0,
            "connect_pct":    0, "hs_connect_pct": 0,
            "said_intro": 0, "had_convo": 0, "asked_meeting": 0, "booked_meeting": 0,
            "icp_a": 0, "icp_b": 0, "icp_c": 0, "icp_x": 0, "icp_ab": 0,
            "icp_ab_pct": 0, "unique_companies": 0,
            "dials_sheet":    kr.get("dials", 0),
            "connects_sheet": kr.get("connects", 0),
            "demos_sheet":    kr.get("demos", 0),
        })
    merged_rows.sort(key=lambda r: (r["iso_date"] or "", r["sdr"]), reverse=True)

    # SDR-level headline KPIs (summed across all data)
    sdr_kpis = {}
    for sdr_name in ["Tim", "Jay"]:
        mine = [r for r in merged_rows if r["sdr"].lower() == sdr_name.lower()]
        if not mine: continue
        total_dials_hs     = sum(r["dials_hs"] for r in mine)
        total_connects_hs  = sum(r["connects_hs"] for r in mine)
        total_hs_only      = sum(r.get("hs_connects", 0) for r in mine)
        total_booked       = sum(r["booked_meeting"] for r in mine)
        total_intro        = sum(r["said_intro"] for r in mine)
        total_convo        = sum(r["had_convo"] for r in mine)
        total_asked        = sum(r["asked_meeting"] for r in mine)
        total_ab           = sum(r["icp_ab"] for r in mine)
        total_companies    = sum(r["unique_companies"] for r in mine)
        total_dials_sheet  = sum(r["dials_sheet"] for r in mine)
        connect_pct        = round(total_connects_hs / total_dials_hs * 100) if total_dials_hs else 0
        icp_ab_pct         = round(total_ab / total_companies * 100) if total_companies else 0
        hs_only_pct        = round(total_hs_only / total_dials_hs * 100) if total_dials_hs else 0
        sdr_kpis[sdr_name] = {
            "dials_hs":        total_dials_hs,
            "connects_hs":     total_connects_hs,
            "hs_connects":     total_hs_only,
            "connect_pct":     connect_pct,
            "hs_connect_pct":  hs_only_pct,
            "said_intro":      total_intro,
            "had_convo":       total_convo,
            "asked_meeting":   total_asked,
            "booked_meeting":  total_booked,
            "icp_ab":          total_ab,
            "icp_ab_pct":      icp_ab_pct,
            "total_companies": total_companies,
            "dials_sheet":     total_dials_sheet,
            "accountability_pct": round(total_dials_hs / total_dials_sheet * 100) if total_dials_sheet else None,
        }

    return {
        "hs_pipeline":    hs_sdr,
        "call_activity":  sdr_totals,
        "recent_activity": recent_kyle[-40:],
        "all_kyle_rows":  all_kyle_rows_export,
        # NEW: HubSpot-sourced call data
        "sdr_kpis":       sdr_kpis,
        "all_hs_rows":    all_hs_rows,
        "daily_merged":   merged_rows,
        "updated_at":     datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source":    "HubSpot calls (sdr_calls) + NocoDB pipeline + Google Sheets",
    }

# ── Data: AE / Sales Review (HubSpot — Natasha + Main, small dataset) ─────────

@_cached("sales_ae", ttl=1800)
def _fetch_sales_ae():
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)
    cutoff_ms = str(int(cutoff.timestamp() * 1000))
    stale_cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    deals = {"active": [], "won": [], "lost": []}
    for pip_id, pip_label in [(PIPELINE_NATASHA,"Natasha"), (PIPELINE_MAIN,"Main Sales")]:
        after = None
        while True:
            body = {
                "filterGroups": [{"filters": [
                    {"propertyName":"pipeline","operator":"EQ","value":pip_id},
                    {"propertyName":"createdate","operator":"GTE","value":cutoff_ms},
                ]}],
                "properties": ["dealname","dealstage","closedate","createdate","amount","hs_lastmodifieddate"],
                "sorts": [{"propertyName":"createdate","direction":"DESCENDING"}],
                "limit": 100,
            }
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            for obj in resp.get("results", []):
                p = obj["properties"]
                sid = p.get("dealstage","")
                created = (p.get("createdate","") or "")[:10]
                modified = (p.get("hs_lastmodifieddate","") or "")[:10]
                entry = {
                    "deal_id":   obj["id"],
                    "name":      p.get("dealname",""),
                    "stage":     NATASHA_STAGES.get(sid, sid),
                    "stage_id":  sid,
                    "pipeline":  pip_label,
                    "close_date":(p.get("closedate","") or "")[:10],
                    "created":   created,
                    "modified":  modified,
                    "amount":    p.get("amount",""),
                    "stale":     modified < stale_cutoff,
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
            "active_count":  len(deals["active"]),
            "won_count":     len(deals["won"]),
            "lost_count":    len(deals["lost"]),
            "stale_count":   sum(1 for d in deals["active"] if d.get("stale")),
            "won_30d":       sum(1 for d in deals["won"]
                                if d["close_date"] >= (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")),
            "won_mtd":       sum(1 for d in deals["won"]
                                if d["close_date"] >= datetime.now().strftime("%Y-%m-01")),
            "pipeline_value": sum(float(d["amount"]) for d in deals["active"] if d.get("amount")),
        },
        "updated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source": "HubSpot (Natasha + Main Sales, last 12 months)",
    }

# ── Data: Onboarding (Google Sheets) ─────────────────────────────────────────
# Sheet structure (fixed layout):
#   Rows 0-8:  title, summary stats (Active/At Risk/Completed/Churned counts)
#   Row 9:     column headers: Company | Key Human | Quote Link | Won Date | Project Manager | MRR | Status
#   Row 10:    "TARGETS" section label (skip)
#   Row 11+:   actual customer records

_ONBOARDING_HEADER_ROW = 9   # 0-indexed
_ONBOARDING_DATA_START  = 11  # 0-indexed

@_cached("onboarding", ttl=1800)
def _fetch_onboarding():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "Onboarding Pipeline", max_rows=200)
    except Exception as e:
        return {"error": str(e), "customers": []}

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""

    # Extract pipeline summary counts from rows 3-6
    summary_map = {}
    for row in rows[3:7]:
        label = v(row, 0)
        val   = v(row, 1)
        if label and val:
            try: summary_map[label] = int(val)
            except: summary_map[label] = val

    # Column headers from row 9
    headers = rows[_ONBOARDING_HEADER_ROW] if len(rows) > _ONBOARDING_HEADER_ROW else []

    # Customer data from row 11 onwards — skip section labels (no MRR value, or single-word rows)
    SKIP_LABELS = {"targets", "active", "at risk", "completed", "churned", ""}
    customers = []
    for row in rows[_ONBOARDING_DATA_START:]:
        name = v(row, 0)
        if not name or name.lower() in SKIP_LABELS: continue
        customers.append({col: v(row, i) for i, col in enumerate(headers) if col})

    # Mark at-risk: explicitly flagged or no activity in 60+ days
    at_risk = [c for c in customers if
        "risk" in c.get("Status","").lower() or c.get("Status","") == "At Risk"]

    phase_counts = Counter(c.get("Status","Active") or "Active" for c in customers)

    return {
        "customers":    customers,
        "total":        len(customers),
        "pipeline_summary": summary_map,   # Active:13, At Risk:0, Completed:1, Churned:0
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

def _require_paul():
    """Gate owner-only endpoints. Falls back to _require_key if PAUL_SECRET unset."""
    _require_key()
    if not PAUL_SECRET: return
    paul = request.args.get("paul") or request.headers.get("X-Paul-Key","")
    if paul != PAUL_SECRET: abort(403)

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
    return jsonify({"ok": True, "version": APP_VERSION,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "data_architecture": "NocoDB + Sheets (HubSpot only for AE pipeline)"})


# ── Data: ICP Audit ───────────────────────────────────────────────────────────

@_cached("icp_audit", ttl=3600)
def _fetch_icp_audit():
    deals = _noco_get_all(NOCO_DEALS_TABLE)

    # Per-company aggregation (deduplicate by company name)
    companies: dict = {}
    for d in deals:
        cn = (d.get("company_name") or "").strip()
        if not cn:
            continue
        if cn not in companies:
            companies[cn] = {
                "company":     cn,
                "industry":    (d.get("industry") or "").strip(),
                "claude_tier": (d.get("icp_tier") or "").strip(),
                "icp_score":   (d.get("icp_score") or "").strip(),
                "researcher":  (d.get("researcher") or "Not set").strip() or "Not set",
                "lead_count":  0,
                "demos":       0,
                "won":         False,
            }
        companies[cn]["lead_count"] += 1
        if d.get("is_demo_booked"):
            companies[cn]["demos"] += 1
        if d.get("is_won"):
            companies[cn]["won"] = True

    # Apply effective tier (Claude if available, else rule-based)
    tier_counts  = {"A": 0, "B": 0, "C": 0, "X": 0}
    claude_count = 0
    rule_count   = 0
    company_list = []

    for cn, c in companies.items():
        claude_tier = c["claude_tier"]
        if claude_tier in ("A","B","C","X"):
            tier = claude_tier
            source = "claude"
            claude_count += 1
        else:
            tier = _rule_icp_tier(c["company"], c["industry"])
            source = "rule"
            rule_count += 1
        c["tier"]       = tier
        c["source"]     = source
        c["score_note"] = _icp_score_note(c)
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        company_list.append(c)

    # Sort: A first, then B, C, X; within tier by company name
    tier_order = {"A": 0, "B": 1, "C": 2, "X": 3}
    company_list.sort(key=lambda x: (tier_order.get(x["tier"],2), x["company"].lower()))

    total = len(company_list)
    ab = tier_counts["A"] + tier_counts["B"]

    # Industry breakdown for A + B companies
    ind_counter: dict = Counter()
    for c in company_list:
        if c["tier"] in ("A","B") and c["industry"]:
            ind_counter[c["industry"]] += 1


    # ── Ops Health: 4-pillar summary ────────────────────────────────────────
    icp_ab_cnt  = sum(1 for d in deals if _effective_icp_tier(d) in ("A","B"))
    demo_booked = sum(1 for d in deals if d.get("is_demo_booked"))
    won         = sum(1 for d in deals if d.get("is_won"))
    no_mobile   = sum(1 for d in deals if d.get("is_no_mobile"))
    try:
        agg = _fetch_sdr_daily_agg()
        total_dials = sum(int(r.get("dials",0) or 0) for r in agg)
        total_conn  = sum(int(r.get("connects",0) or 0) for r in agg)
        sdr_connect_pct = round(total_conn/total_dials*100) if total_dials else 0
        total_intro = sum(int(r.get("said_intro",0) or 0) for r in agg)
        sdr_funnel_pct = round(total_intro/total_conn*100) if total_conn else 0
    except Exception:
        sdr_connect_pct, sdr_funnel_pct = 0, 0
        total_conn = total_dials = 0
    try:
        onb = _fetch_onboarding()
        onb_active  = onb.get("summary",{}).get("active", 0)
        onb_at_risk = onb.get("summary",{}).get("at_risk", 0)
        onb_health_pct = round((onb_active-onb_at_risk)/onb_active*100) if onb_active else 0
    except Exception:
        onb_health_pct, onb_active, onb_at_risk = 0, 0, 0
    def _band(v, good, warn):
        if v >= good: return "good"
        if v >= warn: return "warn"
        return "bad"
    research_icp_pct = round(icp_ab_cnt/total*100) if total else 0
    ae_demo_to_won   = round(won/demo_booked*100) if demo_booked else 0
    ops_health = {
        "research": {
            "label":"Research Quality","metric":"ICP A+B of researched leads",
            "value":f"{research_icp_pct}%","score":research_icp_pct,
            "detail":f"{icp_ab_cnt} of {total} total deals",
            "band":_band(research_icp_pct, 60, 40),
            "sub":f"{no_mobile} ({round(no_mobile/total*100) if total else 0}%) missing phone",
        },
        "sdr": {
            "label":"SDR Quality","metric":"Connect rate · funnel progression",
            "value":f"{sdr_connect_pct}%","score":sdr_connect_pct,
            "detail":f"{total_conn:,} connects of {total_dials:,} dials",
            "band":_band(sdr_connect_pct, 35, 20),
            "sub":f"{sdr_funnel_pct}% of connects reached intro stage",
        },
        "ae": {
            "label":"AE Conversion","metric":"Demo → Won",
            "value":f"{ae_demo_to_won}%","score":ae_demo_to_won,
            "detail":f"{won} wins from {demo_booked} demos booked",
            "band":_band(ae_demo_to_won, 25, 10),
            "sub":f"{active} active deals in pipeline",
        },
        "onboarding": {
            "label":"Onboarding Health","metric":"Customers on track",
            "value":f"{onb_health_pct}%","score":onb_health_pct,
            "detail":f"{onb_active - onb_at_risk} of {onb_active} active",
            "band":_band(onb_health_pct, 85, 65),
            "sub":(f"{onb_at_risk} at-risk" if onb_at_risk else "No at-risk accounts"),
        },
    }

    return {
        "summary": {
            "total_companies": total,
            "tier_a": tier_counts["A"],
            "tier_b": tier_counts["B"],
            "tier_c": tier_counts["C"],
            "tier_x": tier_counts["X"],
            "ab_count": ab,
            "ab_pct": round(ab / total * 100) if total else 0,
            "claude_scored": claude_count,
            "rule_scored": rule_count,
        },
        "companies": company_list[:500],   # cap for browser perf
        "industry_breakdown": dict(ind_counter.most_common(12)),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


@app.route("/api/icp-audit")
def api_icp_audit():
    if request.args.get("force") == "1":
        _bust_cache("icp_audit")
    try:
        return jsonify({"ok": True, "data": _fetch_icp_audit()})
    except Exception as e:
        log.error(f"icp-audit error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Data: Sales Summary (Google Sheets — monthly cohort analysis) ─────────────
# Sheet structure:
#   Rows 2-6:  KPI summary (Total Discos, Won, Evaluation, Lost, MRR, rates)
#   Row 9:     Column headers
#   Row 11+:   Monthly cohort rows

@_cached("sales_summary", ttl=1800)
def _fetch_sales_summary():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "Sales Summary", max_rows=80)
    except Exception as e:
        return {"error": str(e), "cohorts": []}

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""

    # KPIs from rows 2-6
    kpis = {}
    for row in rows[2:7]:
        label = v(row, 0)
        val   = v(row, 1)
        if label and val and label not in ("PIPELINE", "KEY RATES"):
            kpis[label] = val
        # Key rates in cols 5-6
        rate_label = v(row, 5)
        rate_val   = v(row, 6)
        if rate_label and rate_val:
            kpis[rate_label] = rate_val

    # Column headers from row 9
    headers = rows[9] if len(rows) > 9 else []
    col_names = [h.strip() for h in headers]

    # Monthly cohort data — rows 11+ with actual month names
    cohorts = []
    import re
    MONTH_PAT = re.compile(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$')
    for row in rows[11:]:
        month = v(row, 0)
        if not MONTH_PAT.match(month): continue
        discos = v(row, 1)
        if not discos or discos == "0": continue   # skip empty future months
        cohort = {
            "month":      month,
            "discos":     v(row, 1),
            "tim_discos": v(row, 2),
            "jay_discos": v(row, 3),
            "trials":     v(row, 4),
            "trial_pct":  v(row, 5),
            "offers":     v(row, 6),
            "offer_pct":  v(row, 7),
            "won":        v(row, 8),
            "won_pct":    v(row, 9),
        }
        cohorts.append(cohort)

    return {
        "kpis":    kpis,
        "cohorts": list(reversed(cohorts)),   # most recent first
        "updated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_source": "Google Sheets — Sales Summary tab",
    }

@app.route("/api/sales-summary")
def api_sales_summary():
    _require_key()
    if request.args.get("force") == "1": _bust_cache("sales_summary")
    try:
        return jsonify({"ok": True, "data": _fetch_sales_summary()})
    except Exception as e:
        log.error(f"sales-summary error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

# ── Candidate Audit ───────────────────────────────────────────────────────────

_HR_HEADERS = lambda: {"xc-token": NOCO_TOKEN, "Content-Type": "application/json"}


def _fetch_all_candidates():
    rows, page = [], 1
    while True:
        r = requests.get(
            f"{NOCO_BASE_URL}/api/v1/db/data/noco/{NOCO_HR_BASE_ID}/{NOCO_CANDIDATES_TBL}",
            headers=_HR_HEADERS(),
            params={"limit": 100, "offset": (page-1)*100},
        )
        r.raise_for_status()
        d = r.json()
        rows.extend(d.get("list", []))
        if d.get("pageInfo", {}).get("isLastPage", True):
            break
        page += 1
    return rows


def _extract_text_from_file(file) -> str:
    """Extract plain text from a PDF or DOCX file object."""
    name = (file.filename or "").lower()
    data = file.read()

    if name.endswith(".pdf"):
        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                return "\n".join(p.extract_text() or "" for p in pdf.pages)
        except Exception as e:
            return f"[PDF extract error: {e}]"

    if name.endswith(".docx"):
        import io
        try:
            from docx import Document
            doc = Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        except Exception as e:
            return f"[DOCX extract error: {e}]"

    if name.endswith(".txt"):
        return data.decode("utf-8", errors="replace")

    return "[Unsupported file type — upload PDF, DOCX, or TXT]"


def _quick_score_resume(text: str) -> dict:
    """
    Fast keyword-based pre-score. Returns rough SDR/AE/Researcher signals.
    Claude scoring via Alfred gives accurate results — this is a triage preview.
    """
    t = text.lower()
    sdr_kw  = ["cold call","outbound","prospecting","bdr","sdr","pipeline","lead generation",
                "sales development","dial","connect","cadence"]
    ae_kw   = ["account executive","closing","negotiat","quota","revenue","demo","discovery",
                "proposal","contract","full cycle","hunter"]
    res_kw  = ["research","list build","data enrich","linkedin","va ","virtual assistant",
                "lead list","sourcing","data entry","prospecting list"]
    sdr_s  = sum(2 if kw in t else 0 for kw in sdr_kw)
    ae_s   = sum(2 if kw in t else 0 for kw in ae_kw)
    res_s  = sum(2 if kw in t else 0 for kw in res_kw)
    best   = max(sdr_s, ae_s, res_s)
    role   = "SDR" if sdr_s == best else ("AE" if ae_s == best else "Researcher")
    return {"sdr_signal": sdr_s, "ae_signal": ae_s, "researcher_signal": res_s,
            "likely_role": role, "preview": text[:600].strip()}


@app.route("/api/researcher-drill")
def api_researcher_drill():
    """
    Returns the list of companies added on a specific date (or ISO week),
    with ICP tier, score note, and HubSpot contact phone counts.
    Query params: date=YYYY-MM-DD  OR  week=YYYY-Www
    """
    _require_key()
    iso_date = request.args.get("date", "").strip()
    week_key = request.args.get("week", "").strip()

    try:
        deals = _noco_get_all(
            NOCO_DEALS_TABLE,
            fields="company_id,company_name,industry,icp_tier,icp_score,deal_created,is_no_mobile",
        )

        # Filter to date or week
        def _matches(d: dict) -> bool:
            dc = (d.get("deal_created") or "").strip()[:10]
            if iso_date:
                return dc == iso_date
            if week_key:
                try:
                    dt = datetime.strptime(dc, "%Y-%m-%d").date()
                    y, w, _ = dt.isocalendar()
                    return f"{y}-W{w:02d}" == week_key
                except Exception:
                    return False
            return False

        matched = [d for d in deals if _matches(d)]

        # Deduplicate by company_id / name
        seen: set = set()
        unique: list = []
        for d in matched:
            cid  = str(d.get("company_id") or "").strip()
            name = (d.get("company_name") or "").strip().lower()
            key  = cid if cid else name
            if key and key not in seen:
                seen.add(key)
                unique.append(d)

        # Batch-fetch contact phone counts from HubSpot for all company_ids
        company_ids = [str(d["company_id"]) for d in unique if d.get("company_id")]
        phone_counts: dict = {}   # company_id -> int count of contacts with phone

        if company_ids and HUBSPOT_API_KEY:
            try:
                # Step 1: get contact associations for all companies in one batch
                assoc_r = requests.post(
                    "https://api.hubapi.com/crm/v4/associations/companies/contacts/batch/read",
                    headers=_hs_headers(),
                    json={"inputs": [{"id": cid} for cid in company_ids]},
                    timeout=10,
                )
                # company_id -> [contact_id, ...]
                co_contacts: dict = {}
                if assoc_r.ok:
                    for item in assoc_r.json().get("results", []):
                        co_id = str(item.get("from", {}).get("id", ""))
                        co_contacts[co_id] = [str(t["toObjectId"]) for t in item.get("to", [])]

                # Step 2: batch-read contacts that have any association
                all_contact_ids = list({c for ids in co_contacts.values() for c in ids})
                contact_phones: dict = {}  # contact_id -> has phone (bool)
                batch_size = 100
                for i in range(0, len(all_contact_ids), batch_size):
                    batch = all_contact_ids[i:i+batch_size]
                    cr = requests.post(
                        "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
                        headers=_hs_headers(),
                        json={"properties": ["phone", "mobilephone"],
                              "inputs": [{"id": c} for c in batch]},
                        timeout=10,
                    )
                    if cr.ok:
                        for cont in cr.json().get("results", []):
                            p = cont["properties"]
                            has_ph = bool((p.get("phone") or "").strip() or
                                         (p.get("mobilephone") or "").strip())
                            contact_phones[str(cont["id"])] = has_ph

                # Step 3: count contacts-with-phone per company
                for co_id, cids in co_contacts.items():
                    phone_counts[co_id] = sum(1 for c in cids if contact_phones.get(c, False))

            except Exception as e_hs:
                log.warning(f"researcher-drill HubSpot phone fetch failed: {e_hs}")

        # Build response rows
        rows = []
        for d in sorted(unique, key=lambda x: (x.get("company_name") or "").lower()):
            tier    = _effective_icp_tier(d)
            note    = _icp_score_note(d)
            cid_str = str(d.get("company_id") or "")
            phone_c = phone_counts.get(cid_str, None)
            rows.append({
                "company":    d.get("company_name") or "—",
                "industry":   d.get("industry")     or "—",
                "tier":       tier,
                "note":       note,
                "date":       (d.get("deal_created") or "")[:10],
                "phone_count": phone_c,          # int or None
                "no_mobile":  bool(d.get("is_no_mobile")),
            })

        ab  = sum(1 for r in rows if r["tier"] in ("A", "B"))
        pct = round(ab / len(rows) * 100) if rows else 0
        total_with_phone = sum(1 for r in rows if (r["phone_count"] or 0) > 0)

        return jsonify({"ok": True, "data": {
            "date":             iso_date or week_key,
            "total":            len(rows),
            "icp_ab":           ab,
            "icp_pct":          pct,
            "total_with_phone": total_with_phone,
            "rows":             rows,
        }})
    except Exception as e:
        log.error(f"researcher-drill error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/sdr-drill")
def api_sdr_drill():
    """
    Returns all companies an SDR called on a specific date, with ICP tier, # calls,
    last outcome, phone, contact, and funnel stage reached.
    Query params:
      date=YYYY-MM-DD (required)
      sdr=Tim|Jay|all  (default all)
    """
    _require_key()
    iso_date = request.args.get("date", "").strip()
    sdr_filter = (request.args.get("sdr", "all") or "all").strip().lower()
    if not iso_date:
        return jsonify({"ok": False, "error": "date=YYYY-MM-DD required"}), 400

    try:
        agg_rows = _fetch_sdr_daily_agg()
        _STAGE_MAP = {"dial": "no_contact", "said_intro": "intro",
                      "had_convo": "convo", "asked_meeting": "asked",
                      "booked_meeting": "booked"}
        want_sdrs = None
        if sdr_filter != "all":
            want_sdrs = {sdr_filter.title()}

        # Merge drill rows from matching daily_agg rows (one per sdr)
        merged: dict = {}
        for row in agg_rows:
            if (row.get("iso_date") or "")[:10] != iso_date:
                continue
            sdr = (row.get("sdr") or "").strip().title()
            if want_sdrs and sdr not in want_sdrs:
                continue
            blob = row.get("drill_json") or "[]"
            try:
                entries = json.loads(blob) if isinstance(blob, str) else blob
            except Exception:
                entries = []
            for e in entries:
                gkey = (sdr, e.get("company_id") or e.get("company_name") or "")
                m = merged.setdefault(gkey, {
                    "sdr":            sdr,
                    "company_id":     e.get("company_id", ""),
                    "company_name":   e.get("company_name") or "(no company)",
                    "company_domain": e.get("domain", ""),
                    "industry":       e.get("industry", ""),
                    "icp_tier":       (e.get("icp_tier") or "").upper() or "?",
                    "tier":           (e.get("icp_tier") or "").upper() or "?",
                    "icp_note":       e.get("icp_note", ""),
                    "calls":          0,
                    "connects":       0,
                    "duration_ms":    0,
                    "phone":          e.get("contact_phone", ""),
                    "contact":        e.get("contact_name", ""),
                    "last_outcome":   e.get("last_outcome", ""),
                    "last_ts":        e.get("last_ts", ""),
                    "stage_reached":  e.get("stage_reached", "dial"),
                    "reached_stage":  _STAGE_MAP.get(e.get("stage_reached", "dial"), "no_contact"),
                })
                m["calls"]       += int(e.get("calls", 0))
                m["connects"]    += int(e.get("connects", 0))
                m["duration_ms"] += int(e.get("duration_ms", 0))

        tier_order  = {"A":0, "B":1, "C":2, "X":3, "?":4}
        companies   = sorted(merged.values(),
                             key=lambda c: (tier_order.get(c["tier"], 5), -c["calls"]))
        tier_counts = {"A":0,"B":0,"C":0,"X":0,"?":0}
        for c in companies:
            key = c["tier"] if c["tier"] in tier_counts else "?"
            tier_counts[key] += 1
        total_calls = sum(c["calls"] for c in companies)
        ab     = tier_counts["A"] + tier_counts["B"]
        ab_pct = round(ab / len(companies) * 100) if companies else 0

        return jsonify({"ok": True, "data": {
            "date":            iso_date,
            "sdr_filter":      sdr_filter,
            "total_calls":     total_calls,
            "total_companies": len(companies),
            "tier_counts":     tier_counts,
            "icp_ab":          ab,
            "icp_ab_pct":      ab_pct,
            "companies":       companies,
        }})
    except Exception as e:
        log.error(f"sdr-drill error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/candidates")
def api_candidates():
    _require_paul()
    try:
        candidates = _fetch_all_candidates()
        return jsonify({"ok": True, "data": {"candidates": candidates,
                        "total": len(candidates),
                        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}})
    except Exception as e:
        log.error(f"candidates error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/upload-resume", methods=["POST"])
def api_upload_resume():
    _require_paul()
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    f        = request.files["file"]
    name_hint = request.form.get("name", "").strip() or f.filename.rsplit(".", 1)[0].replace("_"," ").title()
    text     = _extract_text_from_file(f)
    signals  = _quick_score_resume(text)

    # Create NocoDB record (Status = Pending Claude Review)
    payload = {
        "Name":           name_hint,
        "Status":         "Pending Claude Review",
        "Top Role Fit":   signals["likely_role"],
        "Audit Notes":    f"Auto-uploaded. Keyword signals → SDR:{signals['sdr_signal']} AE:{signals['ae_signal']} Researcher:{signals['researcher_signal']}. Awaiting Claude scoring.",
        "Cover Letter Quality": "Pending",
    }
    try:
        r = requests.post(
            f"{NOCO_BASE_URL}/api/v1/db/data/noco/{NOCO_HR_BASE_ID}/{NOCO_CANDIDATES_TBL}",
            headers=_HR_HEADERS(), json=payload,
        )
        r.raise_for_status()
        noco_id = r.json().get("Id")
    except Exception as e:
        noco_id = None
        log.warning(f"NocoDB insert failed: {e}")

    return jsonify({"ok": True, "data": {
        "name":       name_hint,
        "noco_id":    noco_id,
        "signals":    signals,
        "preview":    signals["preview"],
        "message":    f"Added '{name_hint}' to NocoDB (ID {noco_id}). Send resume text to Alfred on WhatsApp for Claude scoring.",
    }})


@app.route("/")
def index():
    # paul flag is verified server-side on API calls; we also pass a boolean
    # to the template so we can hide owner-only nav entries for non-owners.
    paul_arg = request.args.get("paul", "")
    is_paul  = bool(PAUL_SECRET) and paul_arg == PAUL_SECRET
    return render_template(
        "index.html",
        dashboard_secret=DASHBOARD_SECRET,
        paul_secret=(paul_arg if is_paul else ""),
        is_paul=is_paul,
        paul_required=bool(PAUL_SECRET),
        app_version=APP_VERSION,
    )

@app.route("/health")
def health():
    return jsonify({"ok": True, "version": APP_VERSION})

if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))
