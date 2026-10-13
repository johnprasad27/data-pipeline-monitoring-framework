"""Application entrypoint for the monitoring framework."""

from __future__ import annotations

import streamlit as st

from pipelines.registry import initialize_pipeline_registry

st.set_page_config(page_title="Data Pipeline Monitor", layout="wide")


@st.cache_data(ttl=300, show_spinner=False)
def _prepare_pipeline_registry() -> bool:
    initialize_pipeline_registry()
    return True


try:
    _prepare_pipeline_registry()
except Exception:
    # The individual database-backed pages display their own connection errors.
    pass

pages = [
    st.Page("pages/overview.py", title="Overview", icon="🏠"),
    st.Page("monitoring/dashboard.py", title="Pipeline Monitor", icon="📊"),
    st.Page("pages/add_pipeline.py", title="Add Pipeline", icon=":material/add_circle:"),
]

navigation = st.navigation(pages, position="sidebar")
navigation.run()
