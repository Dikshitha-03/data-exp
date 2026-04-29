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
def flatten_json(y, parent_key='', sep='.'):
    items = []
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())
    elif isinstance(y, list):
        for i, v in enumerate(y):
            new_key = f"{parent_key}{sep}{i}"
            items.extend(flatten_json(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, y))
    return dict(items)


# Only extract the fields we actually need — much faster
NEEDED_FIELDS = {
    "fields.name", "fields.factor", "fields.source", "fields.year",
    "fields.region", "fields.activity_id", "id",
    "fields.sector", "fields.category", "fields.source_dataset",
    "fields.description", "fields.lca_activity", "fields.scope",
    "fields.unit_type", "fields.co2e_calculation_method",
    "fields.year_released", "fields.data_version",
}

def extract_needed(flat: dict) -> dict:
    return {k: str(v).strip() for k, v in flat.items() if k in NEEDED_FIELDS}


@st.cache_data(show_spinner=False)
def load_data(file_bytes: bytes, filename: str, max_rows: int = 20000) -> pd.DataFrame:
    """Cached: only runs once per uploaded file."""
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
    for i, obj in enumerate(ijson.items(raw, "item")):
        if i >= max_rows:
            break
        data.append(extract_needed(flatten_json(obj)))

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
# Sidebar — renders widgets, stores to session_state immediately
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
    apply  = c1.button("✅ Apply",  use_container_width=True)
    clear  = c2.button("🗑 Clear",  use_container_width=True)

    if clear:
        for k in ["f_keyword","f_sector","f_cat","f_region","f_source",
                  "f_year","f_unit","f_scope","f_lca","f_license","f_version",
                  "active_filters"]:
            st.session_state.pop(k, None)
        st.rerun()

    if apply:
        # Persist filters so they survive reruns
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
        st.rerun()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    filters = st.session_state.get("active_filters", {})
    if not filters:
        return df

    filtered = df.copy()

    # Keyword search
    kw = filters.get("keyword", "")
    if kw:
        name_col = filtered.get("fields.name", pd.Series(dtype=str)) if "fields.name" in filtered else pd.Series([""] * len(filtered))
        desc_col = filtered.get("fields.description", pd.Series(dtype=str)) if "fields.description" in filtered else pd.Series([""] * len(filtered))
        mask = (
            name_col.str.lower().str.contains(kw, na=False) |
            desc_col.str.lower().str.contains(kw, na=False)
        )
        filtered = filtered[mask]

    # Multi-select facets
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

    # Data version
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
        # Read bytes once; cache key is (bytes, filename)
        file_bytes = uploaded_file.read()

        with st.spinner("Loading data… (first load only, then cached)"):
            df = load_data(file_bytes, uploaded_file.name)

        st.success(f"✅ Loaded {len(df):,} records")

        # Render sidebar (widgets write directly to session_state)
        render_filters(df)

        # Apply persisted filters
        filtered_df = apply_filters(df)

        # Show active filter summary
        active = st.session_state.get("active_filters", {})
        if active:
            tags = []
            for k, v in active.items():
                if v and v not in ([], "", "All", "(any)"):
                    label = k.replace("fields.", "").replace("_", " ").title()
                    tags.append(f"**{label}:** {', '.join(v) if isinstance(v, list) else v}")
            if tags:
                st.info("🔎 Active filters: " + " | ".join(tags))

        st.markdown(f"### Results ({len(filtered_df):,})")

        if len(filtered_df) == 0:
            st.warning("No results found. Try different filters.")
            return

        render_table_header()
        for idx, (_, row) in enumerate(filtered_df.iterrows()):
            render_row(row, idx)

    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()