"""Shared fetch button for GSC + GA4 data."""

import streamlit as st

from config import is_ga4_configured, is_gsc_configured


def render_fetch_button():
    """Render a single fetch button that pulls both GSC and GA4 data.

    Disables itself after being clicked. Stores results in session state
    so they persist across reruns.
    """
    already_fetched = st.session_state.get("fetch_done", False)

    col_btn, col_status = st.columns([1, 3])
    with col_btn:
        fetch_clicked = st.button(
            "Fetch GSC + GA4 Data",
            type="primary",
            disabled=already_fetched,
            key="fetch_gsc_ga4",
        )

    if fetch_clicked:
        from google_api import button_date_range, fetch_gsc_data, fetch_ga4_data
        start_date, end_date = button_date_range()
        results = []
        with col_status:
            if is_gsc_configured():
                with st.spinner("Fetching GSC data..."):
                    try:
                        count = fetch_gsc_data(start_date, end_date)
                        results.append(("success", f"GSC: upserted {count:,} rows ({start_date} → {end_date})"))
                    except Exception as e:
                        results.append(("error", f"GSC: {e}"))

            if is_ga4_configured():
                with st.spinner("Fetching GA4 data..."):
                    try:
                        count = fetch_ga4_data(start_date, end_date)
                        results.append(("success", f"GA4: upserted {count:,} rows ({start_date} → {end_date})"))
                    except Exception as e:
                        results.append(("error", f"GA4: {e}"))

        st.session_state["fetch_done"] = True
        st.session_state["fetch_results"] = results
        st.cache_data.clear()
        st.rerun()

    # Show persisted results after rerun
    if already_fetched and "fetch_results" in st.session_state:
        with col_status:
            for level, msg in st.session_state["fetch_results"]:
                if level == "success":
                    st.success(msg)
                elif level == "error":
                    st.error(msg)
                else:
                    st.info(msg)
