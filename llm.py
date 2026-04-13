"""LLM integration for per-section insight summaries."""

import os

import anthropic
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

_client = None


def _get_client() -> anthropic.Anthropic | None:
    global _client
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or api_key == "your-api-key-here":
        return None
    if _client is None:
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def get_section_summary(section_name: str, data_summary: str) -> str | None:
    """Generate an LLM insight summary for a dashboard section.

    Args:
        section_name: e.g. "Search Impressions", "GEO Performance"
        data_summary: A text representation of the key metrics/data for this section.

    Returns:
        The LLM-generated insight string, or None if the API key isn't configured.
    """
    client = _get_client()
    if client is None:
        return None

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": f"""You are a marketing analytics expert reviewing weekly metrics for a B2B SaaS company (Maxim AI / Bifrost).

Analyze the following {section_name} data and provide:
1. Key takeaways (2-3 bullet points)
2. Notable trends or anomalies
3. One actionable recommendation

Be concise and specific. Reference actual numbers from the data.

Data:
{data_summary}""",
            }
        ],
    )
    return message.content[0].text


def render_llm_insights(section_name: str, data_summary: str):
    """Render LLM insights in a Streamlit expander."""
    with st.expander("AI Insights", expanded=False):
        if _get_client() is None:
            st.info("Set `ANTHROPIC_API_KEY` in `.env` to enable AI-powered insights.")
            return

        if st.button(f"Generate insights", key=f"llm_{section_name}"):
            with st.spinner("Analyzing..."):
                summary = get_section_summary(section_name, data_summary)
                if summary:
                    st.markdown(summary)
                else:
                    st.warning("Failed to generate insights.")
