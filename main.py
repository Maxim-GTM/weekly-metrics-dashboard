"""Weekly Metrics Dashboard — Streamlit entry point."""

import streamlit as st

st.set_page_config(
    page_title="Weekly Metrics Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("Weekly Metrics Dashboard")

SECTIONS = {
    "Search Impressions (GSC)": "search_impressions",
    "Traffic Analytics (GA4)": "traffic_analytics",
    "Keyword Performance": "keyword_performance",
    "GEO Performance (Profound)": "geo_profound",
}

selected = st.sidebar.radio("Section", list(SECTIONS.keys()))

# Lazy-import the selected section to avoid loading all at once
if selected == "Search Impressions (GSC)":
    from sections.search_impressions import render
elif selected == "Traffic Analytics (GA4)":
    from sections.traffic_analytics import render
elif selected == "Keyword Performance":
    from sections.keyword_performance import render
elif selected == "GEO Performance (Profound)":
    from sections.geo_profound import render

render()
