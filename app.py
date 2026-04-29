import streamlit as st
import pandas as pd
import ijson
import re
import zipfile
import io

st.set_page_config(layout="wide", page_title="Data Explorer")

st.markdown("""
<style>
    .col-header {
        font-size: 11px; font-weight: 700; text-transform: uppercase;
        color: #888; padding-bottom: 4px;
        border-bottom: 2px solid #e0e0e0; margin-bottom: 6px;
    }
    .factor-badge {
        display: inline-block; background-color: #1A73E8; color: white;
        padding: 2px 14px; border-radius: 12px;
        font-size: 13px; font-weight: 600; white-space: nowrap;
    }
    .kv-table {
        width: 100%; border-collapse: collapse;
        font-size: 13px; margin-bottom: 0;
        border: 1px solid #e8eaed; border-radius: 6px; overflow: hidden;
    }
    .kv-table tr { border-bottom: 1px solid #e8eaed; }
    .kv-table tr:last-child { border-bottom: none; }
    .kv-table td { padding: 10px 14px; vertical-align: middle; }
    .kv-table td:first-child {
        font-size: 11px; font-weight: 700; text-transform: uppercase;
        color: #5f6368; width: 200px; white-space: nowrap; background: #fafbfc;
    }
    .kv-table td:last-child { color: #1a1a1a; background: #fff; }
    .kv-link { color: #1A73E8; font-weight: 500; }
    .meta-header {
        font-size: 11px; font-weight: 700; text-transform: uppercase;
        color: #5f6368; padding: 12px 14px 8px 14px;
        background: #f8f9fa; border: 1px solid #e8eaed;
        border-bottom: none; border-radius: 6px 6px 0 0;
        margin-top: 18px; margin-bottom: 0; display: block;
    }
    .meta-header + .kv-table { border-top: none; border-radius: 0 0 6px 6px; }
    .id-chip {
        background: #f1f3f4; border-radius: 4px;
        padding: 3px 10px; font-family: monospace;
        font-size: 12px; color: #1a1a1a;
        display: inline-block; max-width: 100%; word-break: break-all;
    }
    .desc-block {
        font-size: 13px; color: #3c4043;
        line-height: 1.65; margin-bottom: 14px; padding: 2px 0;
    }
    .status-current { font-weight: 700; color: #188038; }
    .ef-co2 { font-size: 13px; color: #444; }
    .lca-sub { font-size: 11px; color: #888; margin-top: 1px; }
    .row-sep { border: none; border-top: 1px solid #f0f0f0; margin: 3px 0 8px 0; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────

# Only extract the fields we actually need
NEEDED_FIELDS = {
    "fields.name", "fields.factor", "fields.source", "fields.year",
    "fields.region", "fields.activity_id", "id",
    "fields.sector", "fields.category", "fields.source_dataset",
    "fields.description", "fields.lca_activity", "fields.scope",
    "fields.unit_type", "fields.co2e_calculation_method",
    "fields.year_released", "fields.data_version",
}

# Map dotted paths → flat dict keys without full recursive flattening
def extract_from_obj(obj: dict) -> dict:
    """
    Directly extract only the fields we need from a parsed JSON object.
    Much faster than fully flattening every nested key.
    """
    result = {}

    # Top-level id
    if "id" in obj:
        result["id"] = str(obj["id"]).strip()

    # fields.* keys
    fields = obj.get("fields", {})
    if isinstance(fields, dict):
        for key in (
            "name", "factor", "source", "year", "region", "activity_id",
            "sector", "category", "source_dataset", "description",
            "lca_activity", "scope", "unit_type", "co2e_calculation_method",
            "year_released", "data_version",
        ):
            val = fields.get(key)
            if val is not None:
                result[f"fields.{key}"] = str(val).strip()

    return result


@st.cache_data(show_spinner=False)
def load_data(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Load ALL records — no row cap.
    Uses ijson for memory-efficient streaming so even multi-GB files work.
    """
    # Handle zip
    if filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            json_files = [f for f in zf.namelist() if f.endswith(".json")]
            if not json_files:
                raise ValueError("No .json file found inside the zip.")
            with zf.open(json_files[0]) as jf:
                raw = io.BytesIO(jf.read())
    else:
        raw = io.BytesIO(file_bytes)

    data = []
    for obj in ijson.items(raw, "item"):
        data.append(extract_from_obj(obj))

    df = pd.DataFrame(data)
    df = df.fillna("—")
    return df


def safe(row, key):
    v = row.get(key, "—")
    return v if str(v).strip() not in ("", "nan", "None", "—") else "—"


def unique_vals(df, col):
    if col not in df.columns:
        return []
    vals = df[col].replace("—", pd.NA).dropna().unique().tolist()
    return sorted([v for v in vals if str(v).strip()])


# ─────────────────────────────────────────
# Sidebar filters
# ─────────────────────────────────────────
def render_filters(df):
    st.sidebar.header("🔍 Filters")

    st.sidebar.markdown("**KEYWORDS**")
    keyword = st.sidebar.text_input("Search", placeholder="Search…",
                                    label_visibility="collapsed", key="f_keyword")

    st.sidebar.markdown("**SECTOR**")
    sector_sel = st.sidebar.multiselect("Sector", unique_vals(df, "fields.sector"),
                                         label_visibility="collapsed", key="f_sector")

    st.sidebar.markdown("**CATEGORY**")
    cat_sel = st.sidebar.multiselect("Category", unique_vals(df, "fields.category"),
                                      label_visibility="collapsed", key="f_cat")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**FILTER RESULTS**")

    st.sidebar.markdown("**REGION**")
    region_sel = st.sidebar.multiselect("Region", unique_vals(df, "fields.region"),
                                         label_visibility="collapsed", key="f_region")

    st.sidebar.markdown("**SOURCE**")
    source_sel = st.sidebar.multiselect("Source", unique_vals(df, "fields.source"),
                                         label_visibility="collapsed", key="f_source")

    st.sidebar.markdown("**YEAR VALID**")
    year_sel = st.sidebar.multiselect("Year Valid", unique_vals(df, "fields.year"),
                                       label_visibility="collapsed", key="f_year")

    st.sidebar.markdown("**UNIT TYPE**")
    unit_sel = st.sidebar.multiselect("Unit Type", unique_vals(df, "fields.unit_type"),
                                       label_visibility="collapsed", key="f_unit")

    st.sidebar.markdown("**SCOPE**")
    scope_sel = st.sidebar.multiselect("Scope", unique_vals(df, "fields.scope"),
                                        label_visibility="collapsed", key="f_scope")

    st.sidebar.markdown("**LIFECYCLE ACTIVITY**")
    lca_sel = st.sidebar.multiselect("Lifecycle Activity", unique_vals(df, "fields.lca_activity"),
                                      label_visibility="collapsed", key="f_lca")

    st.sidebar.markdown("**LICENSE**")
    license_choice = st.sidebar.radio("License", ["All", "Core", "Premium"],
                                       horizontal=True, label_visibility="collapsed",
                                       key="f_license")

    st.sidebar.markdown("**DATA VERSION**")
    dv_opts = unique_vals(df, "fields.data_version") or ["^24"]
    version_sel = st.sidebar.selectbox("Data Version", ["(any)"] + dv_opts,
                                        label_visibility="collapsed", key="f_version")

    st.sidebar.markdown("---")
    c1, c2 = st.sidebar.columns(2)
    apply = c1.button("✅ Apply", use_container_width=True)
    clear = c2.button("🗑 Clear", use_container_width=True)

    if clear:
        for k in ["f_keyword","f_sector","f_cat","f_region","f_source",
                  "f_year","f_unit","f_scope","f_lca","f_license","f_version",
                  "active_filters", "page_num"]:
            st.session_state.pop(k, None)
        st.rerun()

    if apply:
        st.session_state["active_filters"] = {
            "keyword":               keyword.strip().lower(),
            "fields.sector":         sector_sel,
            "fields.category":       cat_sel,
            "fields.region":         region_sel,
            "fields.source":         source_sel,
            "fields.year":           year_sel,
            "fields.unit_type":      unit_sel,
            "fields.scope":          scope_sel,
            "fields.lca_activity":   lca_sel,
            "license":               license_choice,
            "fields.data_version":   version_sel if version_sel != "(any)" else "",
        }
        st.session_state["page_num"] = 0  # reset to first page on new filter
        st.rerun()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    filters = st.session_state.get("active_filters", {})
    if not filters:
        return df

    filtered = df.copy()

    kw = filters.get("keyword", "")
    if kw:
        name_col = filtered["fields.name"] if "fields.name" in filtered else pd.Series([""] * len(filtered))
        desc_col = filtered["fields.description"] if "fields.description" in filtered else pd.Series([""] * len(filtered))
        mask = (
            name_col.str.lower().str.contains(kw, na=False) |
            desc_col.str.lower().str.contains(kw, na=False)
        )
        filtered = filtered[mask]

    facet_cols = [
        "fields.sector", "fields.category", "fields.region",
        "fields.source", "fields.year", "fields.unit_type",
        "fields.scope", "fields.lca_activity",
    ]
    for col in facet_cols:
        vals = filters.get(col, [])
        if vals and col in filtered.columns:
            clean = [str(v).strip().lower() for v in vals]
            filtered = filtered[
                filtered[col].astype(str).str.strip().str.lower().isin(clean)
            ]

    dv = filters.get("fields.data_version", "")
    if dv and "fields.data_version" in filtered.columns:
        filtered = filtered[filtered["fields.data_version"].str.strip() == dv.strip()]

    return filtered


# ─────────────────────────────────────────
# Table header
# ─────────────────────────────────────────
def render_table_header():
    h = st.columns([4, 1.2, 1.2, 1, 1.2])
    for col, label in zip(h, ["ACTIVITY NAME", "FACTORS", "SOURCE", "YEAR", "REGION"]):
        col.markdown(f'<div class="col-header">{label}</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────
# Single row
# ─────────────────────────────────────────
def render_row(row, idx):
    raw_name      = safe(row, "fields.name")
    factor        = safe(row, "fields.factor")
    source        = safe(row, "fields.source")
    year          = safe(row, "fields.year")
    region        = safe(row, "fields.region")
    activity_id   = safe(row, "fields.activity_id")
    uid           = safe(row, "id")
    sector        = safe(row, "fields.sector")
    category      = safe(row, "fields.category")
    dataset       = safe(row, "fields.source_dataset")
    description   = safe(row, "fields.description")
    lca           = safe(row, "fields.lca_activity")
    scope         = safe(row, "fields.scope")
    unit_type     = safe(row, "fields.unit_type")
    co2_method    = safe(row, "fields.co2e_calculation_method")
    year_released = safe(row, "fields.year_released")

    name = re.sub(r"\[.*?\]", "", raw_name).strip() or raw_name

    cols = st.columns([4, 1.2, 1.2, 1, 1.2])
    cols[1].markdown(f'<span class="factor-badge">{factor}</span>', unsafe_allow_html=True)
    cols[2].markdown(f"<small style='color:#444'>{source}</small>", unsafe_allow_html=True)
    cols[3].markdown(f"<small style='color:#444'>{year}</small>", unsafe_allow_html=True)
    cols[4].markdown(f"<small style='color:#444'>{region}</small>", unsafe_allow_html=True)

    with cols[0]:
        with st.expander(f"▶  {name}"):
            if lca != "—":
                st.markdown(f'<div class="lca-sub">LCA Activity: <code>{lca}</code></div>',
                            unsafe_allow_html=True)

            if description != "—":
                st.markdown(f'<p class="desc-block">{description}</p>', unsafe_allow_html=True)

            st.markdown(f"""
<table class="kv-table">
  <tr><td>SOURCE</td><td><span class="kv-link">{source}</span></td></tr>
  <tr><td>YEAR</td><td>{year}</td></tr>
  <tr><td>YEAR RELEASED</td><td>{year_released}</td></tr>
  <tr><td>REGION</td><td><span class="kv-link">{region}</span></td></tr>
  <tr><td>SOURCE DATASET</td><td>{dataset}</td></tr>
  <tr><td>EMISSION FACTORS</td><td><div class="ef-co2">CO₂e &nbsp;<strong>{factor}</strong></div></td></tr>
  <tr><td>DATA VERSIONING</td><td><span class="status-current">Status: Current</span></td></tr>
</table>
""", unsafe_allow_html=True)

            st.markdown('<span class="meta-header">DETAILED METADATA</span>', unsafe_allow_html=True)
            st.markdown(f"""
<table class="kv-table">
  <tr><td>ACTIVITY ID</td><td><span class="id-chip">{activity_id}</span></td></tr>
  <tr><td>ID</td><td><span class="id-chip">{uid}</span></td></tr>
  <tr><td>SECTOR</td><td><span class="kv-link">{sector}</span></td></tr>
  <tr><td>CATEGORY</td><td><span class="kv-link">{category}</span></td></tr>
  <tr><td>SCOPES</td><td>{scope}</td></tr>
  <tr><td>UNIT TYPE(S)</td><td><span class="kv-link">{unit_type}</span></td></tr>
  <tr><td>CO₂e CALCULATION METHOD</td><td><strong>Methods supported:</strong> {co2_method}</td></tr>
  <tr><td>LCA ACTIVITY</td><td><span class="id-chip">{lca}</span></td></tr>
</table>
""", unsafe_allow_html=True)

    st.markdown("<hr class='row-sep'>", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Pagination controls
# ─────────────────────────────────────────
PAGE_SIZE = 50  # rows rendered per page — tune this for speed vs. scroll comfort

def render_pagination(total: int) -> int:
    """Renders prev/next controls and returns the current page index (0-based)."""
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    if "page_num" not in st.session_state:
        st.session_state["page_num"] = 0

    page = st.session_state["page_num"]
    page = max(0, min(page, total_pages - 1))  # clamp

    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        if st.button("← Prev", disabled=(page == 0), use_container_width=True):
            st.session_state["page_num"] = page - 1
            st.rerun()
    with col2:
        st.markdown(
            f"<div style='text-align:center;padding-top:6px;font-size:13px;color:#555'>"
            f"Page {page + 1} of {total_pages} &nbsp;·&nbsp; {total:,} results</div>",
            unsafe_allow_html=True,
        )
    with col3:
        if st.button("Next →", disabled=(page >= total_pages - 1), use_container_width=True):
            st.session_state["page_num"] = page + 1
            st.rerun()

    return page


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
def main():
    st.title("📊 Data Explorer")

    uploaded_file = st.file_uploader(
        "Upload your JSON or ZIP file", type=["json", "zip"],
        help="ZIP your JSON first to stay under the 200 MB upload limit"
    )

    if not uploaded_file:
        st.info("Upload a .json or .zip file to begin")
        return

    try:
        file_bytes = uploaded_file.read()

        with st.spinner("Loading data… (streamed once, then cached)"):
            df = load_data(file_bytes, uploaded_file.name)

        st.success(f"✅ Loaded {len(df):,} records")

        render_filters(df)
        filtered_df = apply_filters(df)

        # Active filter summary
        active = st.session_state.get("active_filters", {})
        if active:
            tags = []
            for k, v in active.items():
                if v and v not in ([], "", "All", "(any)"):
                    label = k.replace("fields.", "").replace("_", " ").title()
                    tags.append(f"**{label}:** {', '.join(v) if isinstance(v, list) else v}")
            if tags:
                st.info("🔎 Active filters: " + " | ".join(tags))

        total = len(filtered_df)
        if total == 0:
            st.warning("No results found. Try different filters.")
            return

        # ── Pagination ──────────────────────────────
        page = render_pagination(total)
        start = page * PAGE_SIZE
        end   = min(start + PAGE_SIZE, total)
        page_df = filtered_df.iloc[start:end]
        # ─────────────────────────────────────────────

        render_table_header()
        for idx, (_, row) in enumerate(page_df.iterrows()):
            render_row(row, idx)

        # Bottom pagination so you don't have to scroll back up
        render_pagination(total)

    except Exception as e:
        st.error(f"Error: {e}")
        raise  # re-raise in dev so you see the full traceback


if __name__ == "__main__":
    main()