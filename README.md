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
| `ANTHROPIC_API_KEY` | Anthropic API key for LLM insight summaries |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to a Google Cloud service account JSON key file |
| `GSC_PROPERTY` | Google Search Console property (e.g. `sc-domain:example.com`) |
| `GA4_PROPERTY_ID` | GA4 property ID (numeric) |

The Google API variables are optional — if not configured, each section falls back to CSV file upload.

## Usage

```bash
uv run streamlit run main.py
```

The sidebar lets you switch between four sections:

1. **Search Impressions (GSC)** — Google Search Console data, impressions by page category, weekly/monthly trends
2. **Traffic Analytics (GA4)** — GA4 sessions by source, page category drill-down, GEO (AI-referred) traffic
3. **Keyword Performance** — SEMrush + GSC rank tracking from a wide-format CSV, keyword trend charts
4. **GEO Performance (Profound)** — Profound CSV analysis across ChatGPT, AI Overview, and Perplexity; cross-platform overlap and citation analysis

### Data sources

- **GSC & GA4**: Click "Fetch & Analyse" to pull data via the Google APIs (requires env vars above). Results are cached as CSVs in `data/`.
- **Keywords & Profound**: Upload CSVs directly through the file uploader in each section.
- **LLM insights**: Each section summarises its data and calls Claude to generate commentary, shown in an expandable panel.

## Project structure

```
main.py               Streamlit entry point, sidebar navigation
config.py             Page category mapping, Google API config, domain/competitor lists
google_api.py         GSC and GA4 Data API integration
llm.py                Anthropic SDK integration for per-section insights
sections/
  search_impressions.py
  traffic_analytics.py
  keyword_performance.py
  geo_profound.py
```
