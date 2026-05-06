"""
app.py — Main entry point for the Emission Factor Explorer Streamlit app.
Imports and orchestrates db.py (data layer) and ui.py (presentation layer).

Run with:
    streamlit run app.py
"""

import streamlit as st

from db import (
    PAGE_SIZE,
    EF_TABLE_CANDIDATES,
    SRC_TABLE_CANDIDATES,
    discover_tables,
    find_best_candidate,
    build_column_map,
    load_filter_options,
    fetch_grouped_page,
    fetch_children,
    get_connection,
)
from ui import (
    inject_styles,
    render_filters,
    render_pagination,
    render_table_header,
    render_group_row,
)

st.set_page_config(layout="wide", page_title="Emission Factor Explorer")

# ─────────────────────────────────────────
# Filter summary labels
# ─────────────────────────────────────────
SKIP_SUMMARY = {"active_only", "license"}
LABEL_MAP = {
    "keyword":       "Keywords",
    "scope":         "Scope",
    "sector":        "Sector",
    "subcategory":   "Subcategory",
    "category_name": "Category",
    "activity_type": "Activity Type",
    "lca_activity":  "Lifecycle Activity",
    "region":        "Region",
    "country_code":  "Country",
    "source_name":   "Source",
    "year":          "Year Valid",
    "year_released": "Data Version",
    "unit_type":     "Unit Type",
    "data_quality":  "Data Quality",
}

HARDCODED_URL = (
    "postgresql://emission_user:CarbonPassword2026"
    "@emission-pg.postgres.database.azure.com:5432/emission_db?ssl=require"
)


# ─────────────────────────────────────────
# Silent table auto-detection
# ─────────────────────────────────────────
def resolve_tables(db_url: str) -> tuple:
    if "ef_table" in st.session_state and "src_table" in st.session_state:
        return st.session_state["ef_table"], st.session_state["src_table"]

    all_tables = discover_tables(db_url)
    ef_table   = find_best_candidate(all_tables, EF_TABLE_CANDIDATES) or ""
    src_table  = find_best_candidate(all_tables, SRC_TABLE_CANDIDATES) or ""

    st.session_state["ef_table"]  = ef_table
    st.session_state["src_table"] = src_table
    return ef_table, src_table


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
def main():
    inject_styles()

    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
        <div style="font-size:22px;font-weight:700;color:#0f172a;letter-spacing:-0.02em;">
            Emission Factor Explorer
        </div>
        <div style="font-size:11px;background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0;
             border-radius:4px;padding:2px 8px;font-weight:600;letter-spacing:0.04em;">
            READ-ONLY
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Silent auto-connect ──
    if "db_url" not in st.session_state:
        try:
            conn = get_connection(HARDCODED_URL)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            st.session_state["db_url"] = HARDCODED_URL
        except Exception:
            st.error("Could not connect to the database. Please contact support.")
            st.stop()

    db_url = st.session_state["db_url"]

    # ── Silent table detection ──
    ef_table, src_table = resolve_tables(db_url)
    if not ef_table:
        st.error("No emission factor table found. Please contact support.")
        st.stop()

    col_map = build_column_map(db_url, ef_table)

    # ── Filter options ──
    with st.spinner("Loading filters…"):
        try:
            options = load_filter_options(db_url, ef_table, src_table)
        except Exception:
            st.error("Could not load filter options. Please contact support.")
            st.stop()

    active = render_filters(options, col_map)
    st.session_state["active_filters"] = active

    # ── Active filter summary bar ──
    active_tags = []
    for k, v in active.items():
        if k in SKIP_SUMMARY:
            continue
        if v and v not in ([], ""):
            label = LABEL_MAP.get(k, k.replace("_", " ").title())
            active_tags.append(
                f"**{label}:** {', '.join(str(x) for x in v) if isinstance(v, list) else v}"
            )
    if active.get("active_only"):
        active_tags.append("**Status:** Active only")
    if active.get("license", "All") != "All":
        active_tags.append(f"**License:** {active['license']}")
    if active_tags:
        st.info("🔎 " + " | ".join(active_tags))

    # ── Reset page + cached children on filter change ──
    filter_key = str(sorted(str(active.items())))
    if st.session_state.get("_last_filter_key") != filter_key:
        st.session_state["page_num"] = 0
        st.session_state["_last_filter_key"] = filter_key
        # Clear cached child results when filters change
        for k in list(st.session_state.keys()):
            if k.startswith("children_"):
                del st.session_state[k]

    # ── Fetch grouped activities ──
    page = st.session_state.get("page_num", 0)

    with st.spinner("Querying…"):
        try:
            page_df, total, col_map = fetch_grouped_page(
                db_url, ef_table, src_table, active, page)
        except Exception as e:
            st.error("Query error. Please contact support.")
            st.exception(e)
            return

    if total == 0:
        st.warning("No results match your filters.")
        return

    # Build a closure so render_group_row can fetch children without knowing db details
    def get_children(activity_id: str):
        return fetch_children(db_url, ef_table, src_table, activity_id, col_map)

    render_pagination(total, PAGE_SIZE)
    render_table_header()
    for i, (_, row) in enumerate(page_df.iterrows()):
        render_group_row(row, i, get_children)
    render_pagination(total, PAGE_SIZE)


if __name__ == "__main__":
    main()