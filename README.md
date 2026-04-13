# Weekly Metrics Dashboard

A Streamlit dashboard that automates the weekly marketing metrics call for [Maxim AI](https://getmaxim.ai) / [Bifrost](https://getbifrost.ai). Pulls data from Google Search Console, Google Analytics (GA4), SEMrush, and Profound to generate SEO and GEO analysis with LLM-powered insights.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | OpenAI API key for LLM insight summaries and call summary generation |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to a Google Cloud service account JSON key file |
| `GSC_PROPERTY` | Google Search Console property (e.g. `sc-domain:example.com`) |
| `GA4_PROPERTY_ID` | GA4 property ID (numeric) |

GSC and GA4 variables are independent — you can configure one without the other. If neither is set, sections fall back to CSV file upload.

## Usage

```bash
uv run streamlit run main.py
```

The sidebar lets you switch between four sections:

1. **Search Impressions (GSC)** — Impressions by page category, weekly/monthly trends, period-over-period % changes, pages appearing in search
2. **Traffic Analytics (GA4)** — Sessions by source and medium, page category breakdown, per-source drill-down, GEO (AI-referred) traffic with % changes
3. **Keyword Performance** — SEMrush Position Tracking rank data, daily positions, result type distribution, landing page categorization
4. **GEO Performance (Profound)** — Prompt appearance rates across ChatGPT, AI Overview, and Perplexity; cross-platform overlap, owned articles cited, competitor mentions

### Data flow

- **GSC + GA4**: Click "Fetch GSC + GA4 Data" on the Search Impressions or Traffic Analytics page to pull data via Google APIs. Data is saved as date-stamped CSVs in `data/` (e.g. `gsc_2026-03-16_to_2026-04-12.csv`). Fetch windows are aligned to ISO weeks (Mon-Sun), so fetching on any day within the same week produces the same file. If the file already exists, the fetch is skipped.
- **Keywords + Profound**: Upload CSVs through the file uploader in each section. Uploads are automatically saved to `data/` with date-stamped names and loaded from disk on subsequent sessions.
- **LLM insights**: Each section has an "AI Insights" expander that calls OpenAI to generate commentary. The main page has a "Generate Call Summary" button that combines all available data into a structured markdown call doc with a download option.

### Main page features

- **Stale data warning** — Shows a warning if GSC/GA4 data is older than 7 days
- **Generate Call Summary** — One-click LLM-powered summary across all sections, downloadable as markdown

## Project structure

```
main.py                Streamlit entry point, stale data warning, call summary
config.py              Page category mapping, Google API config, CSV discovery helpers
google_api.py          GSC and GA4 Data API integration (ISO week-aligned fetching)
llm.py                 OpenAI SDK integration for per-section and full call summaries
sections/
  fetch_button.py      Shared "Fetch GSC + GA4 Data" button component
  search_impressions.py
  traffic_analytics.py
  keyword_performance.py
  geo_profound.py
data/                  Auto-created directory for all CSVs (gitignored)
```
