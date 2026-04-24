"""Configuration and page category mapping."""

import json
import os
from datetime import date as _date, timedelta as _timedelta

from dotenv import load_dotenv

load_dotenv()

# URL path prefix → display name for page category grouping.
# Used across GSC impressions, GA4 traffic, and GEO traffic breakdowns.
PAGE_CATEGORIES = {
    "/articles": "Articles",
    "/blog": "Blog",
    "/compare": "Comparison Pages",
    "/docs": "Maxim Docs",
    "/products": "Products",
    "/bifrost/llm-cost-calculator": "LLM Cost Calculator",
    "/llm-cost-calculator": "LLM Cost Calculator",
    "/provider-status": "Provider Status",
    "/model-library": "Model Library",
    "/resources": "Resources",
    "/alternatives": "Alternatives",
    "/industry": "Industry Pages",
    "/pricing": "Pricing",
    "/features": "Features",
    # Bifrost documentation (docs.bifrost.ai paths tracked under same GA4 property)
    "/overview": "Bifrost Docs",
    "/quickstart": "Bifrost Docs",
    "/mcp": "Bifrost Docs",
    "/providers": "Bifrost Docs",
    "/deployment-guides": "Bifrost Docs",
    "/api-reference": "Bifrost Docs",
    "/integrations": "Bifrost Docs",
    "/architecture": "Bifrost Docs",
    "/plugins": "Bifrost Docs",
    "/models-catalog": "Bifrost Docs",
    "/evals-handbook": "Bifrost Docs",
    "/contributing": "Bifrost Docs",
    "/benchmarking": "Bifrost Docs",
    "/migration-guides": "Bifrost Docs",
    "/cli-agents": "Bifrost Docs",
    "/changelogs": "Bifrost Docs",
}

# Exact-match pages (checked before prefix matching).
SPECIAL_PAGES = {
    "/": "Maxim Homepage",
    "/enterprise": "Bifrost Enterprise",
    "/bifrost": "Bifrost",
    "/bifrost/enterprise": "Bifrost",
    "/bifrost/book-a-demo": "Bifrost",
    "/bifrost/pricing": "Pricing",
}


def categorize_page(page_path: str) -> str:
    """Map a URL path to its page category."""
    if not isinstance(page_path, str) or not page_path:
        return "Other"

    # Strip query string — GA4's landingPage dimension includes query params
    page_path = page_path.split("?")[0]

    # Check special pages first (exact match)
    for pattern, name in SPECIAL_PAGES.items():
        if page_path.rstrip("/") == pattern.rstrip("/"):
            return name

    # Check explicit prefix matches
    for prefix, name in PAGE_CATEGORIES.items():
        if page_path.startswith(prefix):
            return name

    # Auto-group /bifrost/<segment>/... by the segment name.
    # e.g. /bifrost/resources/mcp-gateway → "Bifrost Resources"
    #      /bifrost/provider-status/azure → "Bifrost Provider Status"
    parts = [p for p in page_path.split("/") if p]
    if len(parts) >= 2 and parts[0] == "bifrost":
        segment = parts[1].replace("-", " ").title()
        return f"Bifrost {segment}"

    # /enterprise/<slug> paths are docs.getbifrost.ai enterprise docs pages
    if len(parts) >= 2 and parts[0] == "enterprise":
        return "Bifrost Docs"

    return "Other"


# Top-level prefixes where the 2nd path segment is a meaningful subcategory.
# e.g. /bifrost/resources/xyz → "Bifrost > resources", but /articles/<slug>
# stays just "Articles" (slug is unique per page, drill-down is noise).
DEEP_CATEGORY_PREFIXES = {"/compare"}


def categorize_page_deep(page_path: str) -> str:
    """Category with one-level drill-down where meaningful.

    e.g. '/bifrost/resources/foo' → 'Bifrost > resources'.
    """
    top = categorize_page(page_path)
    if not isinstance(page_path, str) or not page_path:
        return top
    parts = [p for p in page_path.split("/") if p]
    if not parts:
        return top
    first_prefix = "/" + parts[0]
    if first_prefix in DEEP_CATEGORY_PREFIXES and len(parts) >= 2:
        return f"{top} > {parts[1]}"
    return top


# --- GEO / Profound constants ---

OWNED_DOMAINS = {
    "getmaxim.ai",
    "getbifrost.ai",
    "docs.getbifrost.ai",
    "changelog.getmaxim.ai",
    "epochs.getmaxim.ai",
}

COMPETITOR_DOMAINS = {
    "langfuse.com",
    "langsmith.com",
    "braintrustdata.com",
    "arize.com",
    "helicone.ai",
    "datadoghq.com",
    "newrelic.com",
    "dynatrace.com",
    "honeycomb.io",
    "grafana.com",
    "wandb.ai",
    "portkey.ai",
    "launchdarkly.com",
    "mlflow.org",
}

# Names as they appear in Profound's "mentions" column (lowercased for matching).
COMPETITOR_NAMES = {
    "langfuse",
    "langsmith",
    "braintrust",
    "arize",
    "arize phoenix",
    "arize ai",
    "helicone",
    "portkey",
    "litellm",
    "deepeval",
    "galileo",
    "promptlayer",
    "datadog",
    "newrelic",
    "dynatrace",
    "honeycomb",
    "grafana",
    "weights & biases",
    "prompthub",
    "confident ai",
    "promptfoo",
    "agenta",
    "agentops",
    "ragas",
    "kong",
    "openrouter",
    "truefoundry",
    "fiddler",
}

# Topic → key competitors for head-to-head comparison.
TOPIC_COMPETITORS = {
    "Bifrost": ["litellm", "portkey"],
    "Maxim": ["langfuse", "langsmith", "braintrust", "arize", "helicone"],
}

def week_start(d: _date) -> _date:
    """Return the Sunday that begins the Sun–Sat week containing d."""
    return d - _timedelta(days=d.isoweekday() % 7)


def get_database_url() -> str | None:
    """Return DATABASE_URL from environment, or None if not set."""
    return os.getenv("DATABASE_URL")


def get_google_credentials() -> dict | str | None:
    """Return Google service account credentials.

    Returns:
        - A dict if GOOGLE_SERVICE_ACCOUNT_JSON contains a JSON string (deployment)
        - A file path string if it points to an existing file (local dev)
        - None if not configured
    """
    val = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not val or val.startswith("path/to"):
        return None
    val = val.strip()
    # If it looks like JSON, parse it
    if val.startswith("{"):
        try:
            creds = json.loads(val)
        except json.JSONDecodeError:
            return None
        # Env-var storage often escapes newlines in the private key as literal
        # "\n" (backslash + n). PEM parsing needs real newlines.
        pk = creds.get("private_key")
        if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
            creds["private_key"] = pk.replace("\\n", "\n")
        return creds
    # Otherwise treat as file path
    if not os.path.exists(val):
        return None
    return val


def get_gsc_property() -> str | None:
    return os.getenv("GSC_PROPERTY")


def get_ga4_property_id() -> str | None:
    return os.getenv("GA4_PROPERTY_ID")


def is_gsc_configured() -> bool:
    """True if GSC env vars are set and credentials are available."""
    return get_google_credentials() is not None and get_gsc_property() is not None


def is_ga4_configured() -> bool:
    """True if GA4 env vars are set and credentials are available."""
    return get_google_credentials() is not None and get_ga4_property_id() is not None
