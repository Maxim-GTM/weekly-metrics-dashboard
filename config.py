"""Configuration and page category mapping."""

import os

from dotenv import load_dotenv

load_dotenv()

# URL path prefix → display name for page category grouping.
# Used across GSC impressions, GA4 traffic, and GEO traffic breakdowns.
PAGE_CATEGORIES = {
    "/articles": "Articles",
    "/blog": "Blog",
    "/compare": "Comparison Pages",
    "/docs": "Docs",
    "/products": "Products",
    "/bifrost": "Bifrost",
    "/llm-cost-calculator": "LLM Cost Calculator",
    "/provider-status": "Provider Status",
    "/model-library": "Model Library",
    "/resources": "Resources",
    "/alternatives": "Alternatives",
    "/industry": "Industry Pages",
}

# These are matched separately since they don't follow the path-prefix pattern.
SPECIAL_PAGES = {
    "/": "Bifrost Homepage",
    "/enterprise": "Bifrost Enterprise",
}


def categorize_page(page_path: str) -> str:
    """Map a URL path to its page category."""
    if not page_path:
        return "Other"

    # Check special pages first (exact match)
    for pattern, name in SPECIAL_PAGES.items():
        if page_path.rstrip("/") == pattern.rstrip("/"):
            return name

    # Check path prefixes
    for prefix, name in PAGE_CATEGORIES.items():
        if page_path.startswith(prefix):
            return name

    return "Other"


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

# Data directory and CSV filenames
DATA_DIR = "data"
GSC_CSV_FILENAME = "search_console_data.csv"
GA4_CSV_FILENAME = "analytics_data.csv"


def get_google_credentials_path() -> str | None:
    """Return path to service account JSON, or None if not configured."""
    path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not path or path.startswith("path/to"):
        return None
    if not os.path.exists(path):
        return None
    return path


def get_gsc_property() -> str | None:
    return os.getenv("GSC_PROPERTY")


def get_ga4_property_id() -> str | None:
    return os.getenv("GA4_PROPERTY_ID")


def is_gsc_configured() -> bool:
    """True if GSC env vars are set and the key file exists."""
    return get_google_credentials_path() is not None and get_gsc_property() is not None


def is_ga4_configured() -> bool:
    """True if GA4 env vars are set and the key file exists."""
    return get_google_credentials_path() is not None and get_ga4_property_id() is not None
