"""
FieldInsight Ops Dashboard — Flask backend
Serves the dashboard UI and provides /api/* endpoints.
Data sources: HubSpot API + Google Sheets API.
"""

import os
import json
import time
import base64
import logging
import tempfile
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
CACHE_TTL_SECS    = int(os.getenv("CACHE_TTL", "1800"))   # 30-min default
GOOGLE_SA_B64     = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")  # base64-encoded JSON
GOOGLE_SA_PATH    = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "")  # local path (dev)

# ── Simple in-memory cache ────────────────────────────────────────────────────

_cache: dict = {}

def _cached(key: str, ttl: int = CACHE_TTL_SECS):
    """Decorator: cache the return value of a function for ttl seconds."""
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

# ── HubSpot client ────────────────────────────────────────────────────────────

HS_BASE = "https://api.hubapi.com"

def _hs_headers():
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}

def _hs_post(path, body):
    r = requests.post(f"{HS_BASE}{path}", headers=_hs_headers(), json=body)
    r.raise_for_status()
    return r.json()

def _hs_get(path, params=None):
    r = requests.get(f"{HS_BASE}{path}", headers=_hs_headers(), params=params or {})
    r.raise_for_status()
    return r.json()

# Pipeline constants
PIPELINE_TIM        = "855448889"
PIPELINE_JAY        = "888531086"
PIPELINE_MAIN_SALES = "856313360"
PIPELINE_NATASHA    = "default"

WON_STAGE_IDS  = {"810a68c2-e01d-436b-aaa9-7c81f0760ec5", "1277058739"}
LOST_STAGE_IDS = {"28103468", "1277058740"}

DEMO_BOOKED_STAGE_IDS  = {"1276084793", "1338730826"}
NO_MOBILE_STAGE_IDS    = {"1276084788", "1338730824"}

DISQUALIFIED_STAGE_IDS = {
    "1276084800","1276084792","1276084796","1276084794","1276084801",
    "1276084795","1276084797","1276084798","1276228665","1276228664","1332757886",
    "1338730828","1338730829","1338730830","1338730831","1338730832","1338730833",
    "1338730834","1338730835","1338730836","1338730837","1338730838",
}

STAGE_NAMES = {
    "1276084786":"Not Contacted","1276084789":"1st Cold Call",
    "1276084788":"No Mobile","1321576882":"Unable to Reach",
    "1276084793":"Demo Booked","1276084791":"Future Follow Up",
    "1276084800":"Not Interested","1276228665":"OFF target",
    "1276228664":"Dead Numbers","1332757886":"Overseas",
    "1338730822":"Not Contacted","1338730823":"1st Cold Call",
    "1338730824":"No Mobile","1338730825":"Unable to Reach",
    "1338730826":"Demo Booked","1338730827":"Future Follow Up",
    "1338730828":"Not Interested","1338730836":"OFF target",
    "1338730837":"Dead Numbers","1338730838":"Overseas",
    "1276084792":"[SIMPRO] NI","1276084796":"[ServiceM8] NI",
    "1276084794":"[ASCORA] NI","1276084801":"[FERGUS] NI",
    "1276084795":"[UPTICK] NI","1276084797":"[AroFlo] NI",
    "1276084798":"[ServiceTitan] NI",
    # Natasha pipeline
    "17906279":"Qualification Req","34701073":"Demo Scheduled",
    "f4aca0be-7a50-46f7-9a33-c916ecbc17f6":"Demo Attended",
    "100113677":"Closeable/WIP","113846371":"Proposal Sent",
    "810a68c2-e01d-436b-aaa9-7c81f0760ec5":"WON","28103468":"LOST",
    # Main Sales
    "1277058739":"WON","1277058740":"LOST",
}

KNOWN_RESEARCHERS = ["Tamil","Veera","Mohanapriya","Chinju","Mark Lang","Tim","Jay","Tash","Paul"]

COMPANY_PROPS = [
    "name","domain","phone","numberofemployees","industry","city","state",
    "researcher_for_lead","sdr_demo_booked_by",
    "fieldinsight_icp_score","fieldinsight_icp_tier","createdate",
]

# ── Google Sheets client ──────────────────────────────────────────────────────

def _get_sheets_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    if GOOGLE_SA_B64:
        sa_json = base64.b64decode(GOOGLE_SA_B64).decode()
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    elif GOOGLE_SA_PATH:
        creds = Credentials.from_service_account_file(GOOGLE_SA_PATH, scopes=scopes)
    else:
        raise ValueError("No Google service account configured. Set GOOGLE_SERVICE_ACCOUNT_B64 or GOOGLE_SERVICE_ACCOUNT_PATH.")
    return gspread.authorize(creds)

def _read_sheet_tab(spreadsheet_id, tab_name, max_rows=500):
    gc = _get_sheets_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab_name)
    return ws.get_all_values()[:max_rows]

# ── Data: Pipeline Quality ────────────────────────────────────────────────────

@_cached("pipeline_quality", ttl=1800)
def _fetch_pipeline_quality():
    """Fetch all deals from Tim + Jay, enrich with company data, return structured data."""

    # 1. Build WON company set
    won_companies = {}
    for pip_id, won_stage in [(PIPELINE_NATASHA, "810a68c2-e01d-436b-aaa9-7c81f0760ec5"),
                               (PIPELINE_MAIN_SALES, "1277058739")]:
        after = None
        while True:
            body = {"filterGroups":[{"filters":[
                {"propertyName":"pipeline","operator":"EQ","value":pip_id},
                {"propertyName":"dealstage","operator":"EQ","value":won_stage},
            ]}],"properties":["dealname","closedate","amount"],"limit":100}
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            deal_ids = [r["id"] for r in resp.get("results",[])]
            deals_by_id = {r["id"]: r["properties"] for r in resp.get("results",[])}
            if deal_ids:
                try:
                    assoc = _hs_post("/crm/v4/associations/deals/companies/batch/read",
                                     {"inputs":[{"id":d} for d in deal_ids]})
                    for res in assoc.get("results",[]):
                        fid = res.get("from",{}).get("id","")
                        for t in res.get("to",[]):
                            cid = t.get("toObjectId","") or t.get("id","")
                            if cid and cid not in won_companies:
                                dp = deals_by_id.get(fid,{})
                                won_companies[cid] = {
                                    "won_at": (dp.get("closedate","") or "")[:10],
                                    "pipeline": "Main Sales" if pip_id == PIPELINE_MAIN_SALES else "Natasha",
                                    "deal_name": dp.get("dealname",""),
                                }
                except Exception as e:
                    log.warning(f"WON set error: {e}")
            paging = resp.get("paging",{}).get("next",{})
            after = paging.get("after")
            if not after: break

    # 2. Pull all deals from Tim + Jay
    all_deals = []
    for sdr_name, pip_id in [("Tim", PIPELINE_TIM), ("Jay", PIPELINE_JAY)]:
        after = None
        while True:
            body = {"filterGroups":[{"filters":[
                {"propertyName":"pipeline","operator":"EQ","value":pip_id}
            ]}],"properties":["dealname","dealstage","createdate"],"limit":100}
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            for obj in resp.get("results",[]):
                all_deals.append({"deal_id":obj["id"],"stage_id":obj["properties"].get("dealstage",""),
                                   "deal_name":obj["properties"].get("dealname",""),"sdr":sdr_name,
                                   "createdate":(obj["properties"].get("createdate","") or "")[:10]})
            paging = resp.get("paging",{}).get("next",{})
            after = paging.get("after")
            if not after: break

    log.info(f"Pipeline quality: {len(all_deals)} deals loaded")

    # 3. Batch company associations
    deal_to_company = {}
    for i in range(0, len(all_deals), 100):
        batch = [d["deal_id"] for d in all_deals[i:i+100]]
        try:
            resp = _hs_post("/crm/v4/associations/deals/companies/batch/read",
                            {"inputs":[{"id":d} for d in batch]})
            for res in resp.get("results",[]):
                fid = res.get("from",{}).get("id","")
                to_list = res.get("to",[])
                if to_list:
                    cid = to_list[0].get("toObjectId","") or to_list[0].get("id","")
                    if cid: deal_to_company[fid] = cid
        except Exception as e:
            log.warning(f"Association batch error: {e}")

    # 4. Batch company properties
    unique_cids = list({c for c in deal_to_company.values() if c})
    company_data = {}
    for i in range(0, len(unique_cids), 100):
        batch = unique_cids[i:i+100]
        try:
            resp = _hs_post("/crm/v3/objects/companies/batch/read",
                            {"inputs":[{"id":c} for c in batch],"properties":COMPANY_PROPS})
            for obj in resp.get("results",[]):
                company_data[obj["id"]] = obj.get("properties",{})
        except Exception as e:
            log.warning(f"Company batch error: {e}")

    # 5. Enrich deals
    enriched = []
    stage_counts = Counter()
    for deal in all_deals:
        cid = deal_to_company.get(deal["deal_id"],"")
        co  = company_data.get(cid,{}) if cid else {}
        sid = deal["stage_id"]
        stage_counts[STAGE_NAMES.get(sid, sid)] += 1
        enriched.append({
            "deal_id":    deal["deal_id"],
            "company":    (co.get("name") or "").strip() or deal["deal_name"].replace(" - New Deal",""),
            "company_id": cid,
            "researcher": (co.get("researcher_for_lead") or "").strip(),
            "sdr":        deal["sdr"],
            "sdr_demo":   (co.get("sdr_demo_booked_by") or "").strip(),
            "stage":      STAGE_NAMES.get(sid, sid),
            "stage_id":   sid,
            "icp_tier":   (co.get("fieldinsight_icp_tier") or "").strip(),
            "icp_score":  co.get("fieldinsight_icp_score") or "",
            "phone":      bool(co.get("phone","")),
            "employees":  co.get("numberofemployees",""),
            "industry":   (co.get("industry") or "").strip(),
            "createdate": deal["createdate"],
            "is_disqualified": sid in DISQUALIFIED_STAGE_IDS,
            "is_demo_booked":  sid in DEMO_BOOKED_STAGE_IDS,
            "is_no_mobile":    sid in NO_MOBILE_STAGE_IDS,
            "is_won":          cid in won_companies,
            "won_at":          won_companies.get(cid,{}).get("won_at",""),
            "won_pipeline":    won_companies.get(cid,{}).get("pipeline",""),
        })

    # 6. Aggregate by researcher
    by_researcher = defaultdict(list)
    for d in enriched:
        by_researcher[d["researcher"] or "Not set"].append(d)

    researcher_stats = []
    for researcher in (KNOWN_RESEARCHERS + ["Not set"]):
        deals = by_researcher.get(researcher)
        if not deals: continue
        total   = len(deals)
        active  = sum(1 for d in deals if not d["is_disqualified"])
        no_mob  = sum(1 for d in deals if d["is_no_mobile"])
        demos   = sum(1 for d in deals if d["is_demo_booked"])
        won     = sum(1 for d in deals if d["is_won"])
        icp_ab  = sum(1 for d in deals if d["icp_tier"] in ("A","B"))
        phone_ok = active - no_mob
        researcher_stats.append({
            "name":       researcher,
            "total":      total,
            "active":     active,
            "no_mobile":  no_mob,
            "phone_pct":  round(phone_ok/active*100) if active else 0,
            "demos":      demos,
            "won":        won,
            "icp_ab":     icp_ab,
            "icp_pct":    round(icp_ab/total*100) if total else 0,
            "demo_rate":  round(demos/active*100) if active else 0,
            "win_rate":   round(won/total*100) if total else 0,
        })

    total = len(enriched)
    active = sum(1 for d in enriched if not d["is_disqualified"])

    return {
        "summary": {
            "total":        total,
            "active":       active,
            "disqualified": total - active,
            "demo_booked":  sum(1 for d in enriched if d["is_demo_booked"]),
            "no_mobile":    sum(1 for d in enriched if d["is_no_mobile"]),
            "won":          sum(1 for d in enriched if d["is_won"]),
            "icp_ab":       sum(1 for d in enriched if d["icp_tier"] in ("A","B")),
            "competitor":   sum(1 for d in enriched if "NI" in d["stage"] or "Not Interested" in d["stage"] and d.get("stage","").startswith("[")),
            "no_mobile_pct": round(sum(1 for d in enriched if d["is_no_mobile"])/total*100) if total else 0,
        },
        "by_researcher": researcher_stats,
        "stage_counts":  dict(stage_counts.most_common(12)),
        "won_deals":     sorted(
            [d for d in enriched if d["is_won"]],
            key=lambda x: x["won_at"], reverse=True
        )[:25],
        "demo_deals":    [d for d in enriched if d["is_demo_booked"]][:20],
        "updated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── Data: Researcher activity (from Google Sheets) ────────────────────────────

@_cached("researcher_activity", ttl=1800)
def _fetch_researcher_activity():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "2026 SDR ", max_rows=600)
    except Exception as e:
        return {"error": str(e), "rows": []}

    RESEARCHERS = {"chinju","veera","mohanapriya","tamil"}
    data_rows = rows[2:] if len(rows) > 2 else []
    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""
    def n(row, i):
        try: return int(row[i].strip()) if len(row) > i and row[i].strip() else 0
        except: return 0

    by_researcher = defaultdict(lambda: {"leads":0,"dials":0,"emails":0,"connects":0,"demos":0,"days":0})
    dates_seen = []
    for row in data_rows:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in RESEARCHERS: continue
        date = v(row,0)
        if date not in dates_seen: dates_seen.append(date)
        r = by_researcher[v(row,1)]
        r["leads"]    += n(row,2)
        r["dials"]    += n(row,4)
        r["emails"]   += n(row,5)
        r["connects"] += n(row,6)
        r["demos"]    += n(row,8)
        r["days"]     += 1

    researchers = []
    for name, stats in by_researcher.items():
        researchers.append({
            "name":     name,
            "leads":    stats["leads"],
            "dials":    stats["dials"],
            "emails":   stats["emails"],
            "connects": stats["connects"],
            "demos":    stats["demos"],
            "days":     stats["days"],
            "connect_rate": round(stats["connects"]/stats["dials"]*100) if stats["dials"] else 0,
            "demo_rate":    round(stats["demos"]/stats["connects"]*100) if stats["connects"] else 0,
        })

    recent = []
    for row in data_rows:
        if len(row) < 2 or not v(row,0) or not v(row,1): continue
        rep = v(row,1).lower()
        if rep not in RESEARCHERS: continue
        if v(row,0) in dates_seen[-5:]:
            recent.append({"date":v(row,0),"rep":v(row,1),"leads":n(row,2),
                           "dials":n(row,4),"emails":n(row,5),"connects":n(row,6),"demos":n(row,8)})

    return {
        "totals":     researchers,
        "recent":     recent,
        "date_range": f"{dates_seen[0] if dates_seen else '—'} – {dates_seen[-1] if dates_seen else '—'}",
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── Data: SDR activity (from Google Sheets + HubSpot stage counts) ────────────

@_cached("sdr_activity", ttl=1800)
def _fetch_sdr_activity():
    # HubSpot stage breakdown
    hs_sdr = {}
    for sdr_name, pip_id in [("Tim", PIPELINE_TIM), ("Jay", PIPELINE_JAY)]:
        stage_counts = Counter()
        after = None
        while True:
            body = {"filterGroups":[{"filters":[{"propertyName":"pipeline","operator":"EQ","value":pip_id}]}],
                    "properties":["dealstage"],"limit":100}
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            for r in resp.get("results",[]): stage_counts[r["properties"].get("dealstage","")] += 1
            paging = resp.get("paging",{}).get("next",{})
            after = paging.get("after")
            if not after: break
        hs_sdr[sdr_name] = dict(stage_counts)

    def _stage_summary(counts):
        total         = sum(counts.values())
        not_contacted = counts.get("1276084786",0) + counts.get("1338730822",0)
        called        = counts.get("1276084789",0) + counts.get("1338730823",0)
        no_mobile     = counts.get("1276084788",0) + counts.get("1338730824",0)
        unable        = counts.get("1321576882",0) + counts.get("1338730825",0)
        demo          = counts.get("1276084793",0) + counts.get("1338730826",0)
        follow_up     = counts.get("1276084791",0) + counts.get("1338730827",0)
        not_int       = sum(v for k,v in counts.items() if k in DISQUALIFIED_STAGE_IDS)
        return {
            "total": total, "not_contacted": not_contacted, "called": called,
            "no_mobile": no_mobile, "unable_to_reach": unable,
            "demo_booked": demo, "future_followup": follow_up,
            "disqualified": not_int,
            "demo_rate_pct": round(demo/(called+unable+follow_up+demo)*100) if (called+unable+follow_up+demo) else 0,
            "phone_quality_pct": round((total-not_int-no_mobile)/(total-not_int)*100) if (total-not_int) else 0,
        }

    # Kyle Tracking sheet
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
        r["dials"]    += n(row,2)
        r["connects"] += n(row,3)
        r["demos"]    += n(row,4)
        r["days"]     += 1
        if date in dates_seen[-7:]:
            recent_kyle.append({"date":date,"rep":v(row,1),"dials":n(row,2),
                                 "connects":n(row,3),"demos":n(row,4)})

    sdr_totals = []
    for sdr, stats in kyle_by_sdr.items():
        sdr_totals.append({
            "name": sdr, "dials": stats["dials"], "connects": stats["connects"],
            "demos": stats["demos"], "days": stats["days"],
            "connect_rate": round(stats["connects"]/stats["dials"]*100) if stats["dials"] else 0,
            "demo_rate":    round(stats["demos"]/stats["connects"]*100) if stats["connects"] else 0,
        })

    return {
        "hs_pipeline": {
            "Tim": _stage_summary(hs_sdr.get("Tim",{})),
            "Jay": _stage_summary(hs_sdr.get("Jay",{})),
        },
        "call_activity": sdr_totals,
        "recent_activity": recent_kyle[-40:],
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── Data: Sales AE Review (Natasha + Main Sales) ─────────────────────────────

@_cached("sales_ae", ttl=1800)
def _fetch_sales_ae():
    NATASHA_STAGES = {
        "17906279":"Qualification Req","34701073":"Demo Scheduled",
        "f4aca0be-7a50-46f7-9a33-c916ecbc17f6":"Demo Attended",
        "100113677":"Closeable/WIP","113846371":"Proposal Sent",
        "810a68c2-e01d-436b-aaa9-7c81f0760ec5":"WON","28103468":"LOST",
    }

    deals = {"active":[], "won":[], "lost":[]}
    for pip_id, pip_label in [(PIPELINE_NATASHA, "Natasha"), (PIPELINE_MAIN_SALES, "Main Sales")]:
        after = None
        while True:
            body = {"filterGroups":[{"filters":[{"propertyName":"pipeline","operator":"EQ","value":pip_id}]}],
                    "properties":["dealname","dealstage","closedate","createdate","hubspot_owner_id","amount"],
                    "sorts":[{"propertyName":"createdate","direction":"DESCENDING"}],"limit":100}
            if after: body["after"] = after
            resp = _hs_post("/crm/v3/objects/deals/search", body)
            for obj in resp.get("results",[]):
                p = obj["properties"]
                sid = p.get("dealstage","")
                stage_name = NATASHA_STAGES.get(sid, STAGE_NAMES.get(sid, sid))
                entry = {
                    "deal_id":   obj["id"],
                    "name":      p.get("dealname",""),
                    "stage":     stage_name,
                    "stage_id":  sid,
                    "pipeline":  pip_label,
                    "close_date":(p.get("closedate","") or "")[:10],
                    "created":   (p.get("createdate","") or "")[:10],
                    "amount":    p.get("amount",""),
                }
                if sid in WON_STAGE_IDS:  deals["won"].append(entry)
                elif sid in LOST_STAGE_IDS: deals["lost"].append(entry)
                else: deals["active"].append(entry)
            paging = resp.get("paging",{}).get("next",{})
            after = paging.get("after")
            if not after: break

    # Stage funnel for Natasha
    stage_order = ["Qualification Req","Demo Scheduled","Demo Attended","Closeable/WIP","Proposal Sent"]
    stage_counts = Counter(d["stage"] for d in deals["active"])
    funnel = [{"stage": s, "count": stage_counts.get(s,0)} for s in stage_order]

    return {
        "active":    deals["active"][:50],
        "won":       sorted(deals["won"], key=lambda x: x["close_date"], reverse=True)[:30],
        "lost":      sorted(deals["lost"], key=lambda x: x["close_date"], reverse=True)[:20],
        "funnel":    funnel,
        "summary": {
            "active_count": len(deals["active"]),
            "won_count":    len(deals["won"]),
            "lost_count":   len(deals["lost"]),
            "won_30d":      sum(1 for d in deals["won"] if d["close_date"] >= (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")),
        },
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── Data: Onboarding Review ───────────────────────────────────────────────────

@_cached("onboarding", ttl=1800)
def _fetch_onboarding():
    try:
        rows = _read_sheet_tab(SALES_SHEET_ID, "Onboarding Pipeline", max_rows=200)
    except Exception as e:
        return {"error": str(e), "customers": []}

    def v(row, i):
        try: return row[i].strip() if len(row) > i else ""
        except: return ""

    customers = []
    headers = rows[0] if rows else []
    for row in rows[1:]:
        if len(row) < 2 or not v(row, 0): continue
        customers.append({col: v(row, i) for i, col in enumerate(headers) if col})

    phase_counts = Counter(c.get("Phase","") or c.get("Status","") or "Unknown" for c in customers if c)
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
    """Allow requests with ?key=SECRET or header X-Dashboard-Key"""
    if not DASHBOARD_SECRET:
        return   # no secret set = open (dev mode)
    key = request.args.get("key") or request.headers.get("X-Dashboard-Key","")
    if key != DASHBOARD_SECRET:
        abort(403)

@app.route("/api/pipeline-quality")
def api_pipeline_quality():
    _require_key()
    force = request.args.get("force") == "1"
    if force: _bust_cache("pipeline_quality")
    try:
        return jsonify({"ok": True, "data": _fetch_pipeline_quality()})
    except Exception as e:
        log.error(f"pipeline-quality error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/researchers")
def api_researchers():
    _require_key()
    force = request.args.get("force") == "1"
    if force: _bust_cache("researcher_activity")
    try:
        return jsonify({"ok": True, "data": _fetch_researcher_activity()})
    except Exception as e:
        log.error(f"researchers error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/sdr")
def api_sdr():
    _require_key()
    force = request.args.get("force") == "1"
    if force: _bust_cache("sdr_activity")
    try:
        return jsonify({"ok": True, "data": _fetch_sdr_activity()})
    except Exception as e:
        log.error(f"sdr error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/sales")
def api_sales():
    _require_key()
    force = request.args.get("force") == "1"
    if force: _bust_cache("sales_ae")
    try:
        return jsonify({"ok": True, "data": _fetch_sales_ae()})
    except Exception as e:
        log.error(f"sales error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/onboarding")
def api_onboarding():
    _require_key()
    force = request.args.get("force") == "1"
    if force: _bust_cache("onboarding")
    try:
        return jsonify({"ok": True, "data": _fetch_onboarding()})
    except Exception as e:
        log.error(f"onboarding error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "version": "1.0.0", "ts": datetime.now(timezone.utc).isoformat()})

@app.route("/")
def index():
    return render_template("index.html",
                           dashboard_secret=DASHBOARD_SECRET,
                           version="1.0.0")

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))
