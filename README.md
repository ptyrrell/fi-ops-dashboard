# FieldInsight Ops Dashboard

A live sales operations dashboard for FieldInsight — built with Flask, deployed on Heroku.

## Views

| View | Data Source |
|------|------------|
| **Pipeline Quality** | HubSpot — Tim + Jay SDR pipelines |
| **Researcher Review** | Google Sheets — 2026 SDR tab |
| **SDR Review** | HubSpot pipelines + Kyle Tracking sheet |
| **AE Review** | HubSpot — Natasha + Main Sales pipeline |
| **Onboarding** | Google Sheets — Onboarding Pipeline tab |

## Setup

### 1. Clone and install

```bash
git clone https://github.com/ptyrrell/fi-ops-dashboard.git
cd fi-ops-dashboard
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `HUBSPOT_API_KEY` | HubSpot Service Key (needs CRM read scopes) |
| `SALES_SHEET_ID` | Google Sheets ID (default: FieldInsight 2026 Sales) |
| `DASHBOARD_SECRET` | Optional password to protect the API endpoints |
| `GOOGLE_SERVICE_ACCOUNT_B64` | Base64-encoded service account JSON (for Heroku) |
| `GOOGLE_SERVICE_ACCOUNT_PATH` | Path to service account JSON (for local dev) |
| `CACHE_TTL` | Cache duration in seconds (default: 1800 = 30 min) |

### 3. Run locally

```bash
python app.py
# → http://localhost:5000
```

### 4. Google service account (Heroku)

To encode your service account JSON for Heroku:

```bash
base64 -i your-service-account.json | pbcopy
# Then set: heroku config:set GOOGLE_SERVICE_ACCOUNT_B64="<paste>"
```

## Deploying to Heroku

```bash
heroku create fi-ops-dashboard
heroku config:set HUBSPOT_API_KEY="your_key_here"
heroku config:set DASHBOARD_SECRET="your_secret_here"
heroku config:set GOOGLE_SERVICE_ACCOUNT_B64="$(base64 -i your-service-account.json)"
git push heroku main
```

## Architecture

- **Flask** backend serves the dashboard UI + `/api/*` endpoints
- **In-memory cache** (30 min TTL) reduces API calls to HubSpot/Sheets
- **Single-page dashboard** with 5 views, loads data on demand
- Force-refresh any panel with the `↺ Refresh` button (busts cache)

## API Endpoints

All endpoints accept `?key=DASHBOARD_SECRET` for authentication.

| Endpoint | Description |
|----------|-------------|
| `GET /api/pipeline-quality` | SDR pipeline quality by researcher |
| `GET /api/researchers` | Researcher activity from Sheets |
| `GET /api/sdr` | SDR pipeline stage counts + call activity |
| `GET /api/sales` | AE pipeline — active, won, lost |
| `GET /api/onboarding` | Onboarding pipeline |
| `GET /api/health` | Health check |

Add `&force=1` to any endpoint to bypass cache.
