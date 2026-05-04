import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import re
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide", page_title="Emission Factor Explorer")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }

    .col-header {
        font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em;
        color: #94a3b8; padding-bottom: 6px;
        border-bottom: 1px solid #e2e8f0; margin-bottom: 8px;
    }
    .scope-badge {
        display: inline-block; padding: 2px 9px; border-radius: 4px;
        font-size: 11px; font-weight: 700; white-space: nowrap; letter-spacing: 0.04em;
        max-width: 100%; overflow: hidden; text-overflow: ellipsis;
    }
    .scope-1 { background:#dcfce7; color:#14532d; }
    .scope-2 { background:#dbeafe; color:#1e3a8a; }
    .scope-3 { background:#fce7f3; color:#831843; }
    .scope-other { background:#f1f5f9; color:#475569; }
    .kv-table {
        width: 100%; border-collapse: collapse; font-size: 12.5px; margin-bottom: 0;
        border: 1px solid #e2e8f0; border-radius: 6px; overflow: hidden;
    }
    .kv-table tr { border-bottom: 1px solid #f1f5f9; }
    .kv-table tr:last-child { border-bottom: none; }
    .kv-table td { padding: 8px 14px; vertical-align: middle; }
    .kv-table td:first-child {
        font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em;
        color: #64748b; width: 160px; min-width: 160px; white-space: nowrap; background: #f8fafc;
    }
    .kv-table td:last-child {
        color: #1e293b; background: #fff; font-size: 13px;
        font-family: 'IBM Plex Sans', sans-serif; font-weight: 400;
        word-break: break-word;
    }
    .kv-link { color: #2563eb; font-weight: 500; text-decoration: none; }
    .meta-header {
        font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em;
        color: #64748b; padding: 10px 14px 7px 14px;
        background: #f8fafc; border: 1px solid #e2e8f0;
        border-bottom: none; border-radius: 6px 6px 0 0;
        margin-top: 16px; margin-bottom: 0; display: block;
    }
    .meta-header + .kv-table { border-top: none; border-radius: 0 0 6px 6px; }
    .id-chip {
        background: #f1f5f9; border-radius: 3px; padding: 2px 8px;
        font-family: 'IBM Plex Mono', monospace !important; font-size: 11px; color: #334155;
        display: inline-block; max-width: 100%; word-break: break-all;
    }
    .desc-block { font-size: 13px; color: #475569; line-height: 1.7; margin-bottom: 14px; }
    .status-active   { font-weight: 700; color: #16a34a; }
    .status-inactive { font-weight: 700; color: #dc2626; }
    .ef-grid { display: flex; flex-wrap: wrap; gap: 8px; margin: 8px 0 12px 0; }
    .ef-pill {
        background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 6px;
        padding: 4px 11px; font-size: 12px; color: #0369a1;
    }
    .ef-pill span { font-weight: 700; color: #0c4a6e; font-family: 'IBM Plex Mono', monospace; }
    .lca-sub { font-size: 11px; color: #94a3b8; margin-top: 2px; margin-bottom: 4px; }
    .row-sep { border: none; border-top: 1px solid #f1f5f9; margin: 2px 0 6px 0; }
    .quality-bar-wrap { display:inline-block; width:72px; height:6px;
        background:#e2e8f0; border-radius:3px; vertical-align:middle; margin-right:7px; }
    .quality-bar { height:6px; border-radius:3px; background:#3b82f6; }
    .col-mapping-info { background:#fffbeb; border:1px solid #fcd34d; border-radius:6px;
        padding:10px 14px; margin-bottom:12px; font-size:12px; color:#92400e; }
    .filter-tag { background:#eff6ff; border:1px solid #bfdbfe; border-radius:4px;
        padding:2px 8px; font-size:11px; color:#1d4ed8; display:inline-block; margin:2px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
PAGE_SIZE = 50

EF_TABLE_CANDIDATES = [
    "emissions_emissionfactor", "emission_emissionfactor",
    "api_emissionfactor", "core_emissionfactor", "emissionfactor",
    "emission_factors", "emissionfactors",
]
SRC_TABLE_CANDIDATES = [
    "emissions_factorsource", "emission_factorsource",
    "api_factorsource", "core_factorsource", "factorsource",
    "factor_sources", "factorsources", "sources",
]

# ── Column name aliases: canonical → possible DB column names (priority order)
COLUMN_ALIASES = {
    "id":                   ["id", "uuid", "pk"],
    "climatiq_id":          ["climatiq_id", "activity_id", "external_id", "factor_id"],
    "external_ref":         ["external_ref", "external_reference", "ref"],
    "scope":                ["scope", "ghg_scope", "emission_scope"],
    "subcategory":          ["subcategory", "sub_category", "sector", "category_sector"],
    "category_name":        ["category_name", "category", "activity_category"],
    "name":                 ["name", "activity_name", "factor_name", "title"],
    "activity_type":        ["activity_type", "type", "factor_type"],
    "lca_activity":         ["lca_activity", "lca_stage", "lifecycle_activity", "lifecycle_stage"],
    "description":          ["description", "notes", "details"],
    "region":               ["region", "region_code", "geo_region"],
    "region_name":          ["region_name", "region_label", "geography"],
    "country_code":         ["country_code", "country", "iso_country"],
    "year":                 ["year", "data_year", "reference_year", "valid_year"],
    "year_released":        ["year_released", "release_year", "published_year"],
    "source_id":            ["source_id", "source", "data_source_id"],
    "source_reference":     ["source_reference", "source_ref", "reference"],
    "co2e_factor":          ["co2e_factor", "co2e", "factor", "emission_factor", "ghg_factor", "co2_equivalent"],
    "co2_factor":           ["co2_factor", "co2", "carbon_dioxide"],
    "ch4_factor":           ["ch4_factor", "ch4", "methane"],
    "n2o_factor":           ["n2o_factor", "n2o", "nitrous_oxide"],
    "biogenic_co2_factor":  ["biogenic_co2_factor", "biogenic_co2", "biogenic"],
    "unit_type":            ["unit_type", "unit_category", "measurement_type"],
    "factor_unit":          ["factor_unit", "unit", "emission_unit", "kg_co2e_per"],
    "input_unit":           ["input_unit", "activity_unit", "per_unit"],
    "confidence_score":     ["confidence_score", "confidence", "quality_score"],
    "data_quality":         ["data_quality", "quality", "data_grade"],
    "tags":                 ["tags", "labels", "keywords"],
    "is_custom":            ["is_custom", "custom", "user_defined"],
    "is_active":            ["is_active", "active", "enabled", "status"],
    "created_at":           ["created_at", "created", "date_created"],
    "updated_at":           ["updated_at", "updated", "date_updated", "modified_at"],
}

# ─────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────
def parse_db_url(url: str) -> dict:
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError("URL must start with postgresql:// or postgres://")
    return {
        "host":     parsed.hostname,
        "port":     parsed.port or 5432,
        "user":     parsed.username,
        "password": parsed.password,
        "dbname":   parsed.path.lstrip("/").split("?")[0],
        "sslmode":  "require",
        "connect_timeout": 10,
        "options":  "-c default_transaction_read_only=on",
    }


@st.cache_resource(show_spinner=False)
def get_connection(db_url: str):
    conn = psycopg2.connect(**parse_db_url(db_url))
    conn.autocommit = True
    return conn


def run_query(db_url: str, sql: str, params=None) -> pd.DataFrame:
    conn = get_connection(db_url)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_connection.clear()
        conn = get_connection(db_url)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


# ─────────────────────────────────────────
# Table & column discovery
# ─────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=120)
def discover_tables(db_url: str) -> list:
    df = run_query(db_url,
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
    return df["tablename"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=300)
def get_table_columns(db_url: str, table: str) -> list:
    df = run_query(db_url,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
        (table,))
    return df["column_name"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=300)
def build_column_map(db_url: str, table: str) -> dict:
    """
    Returns a dict: canonical_name → actual_db_column_name
    Only includes mappings where a real column was found.
    """
    actual_cols = set(get_table_columns(db_url, table))
    mapping = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in actual_cols:
                mapping[canonical] = alias
                break
    return mapping


def find_best_candidate(all_tables: list, candidates: list):
    for c in candidates:
        if c in all_tables:
            return c
    return None


# ─────────────────────────────────────────
# Fast parallel filter options loading
# ─────────────────────────────────────────
def _load_one_filter(args):
    db_url, ef_table, col_map, canonical, join_sql, src_table = args
    actual = col_map.get(canonical)
    if not actual:
        return canonical, []
    try:
        df = run_query(db_url,
            f'SELECT DISTINCT ef."{actual}" FROM "{ef_table}" ef '
            f'WHERE ef."{actual}" IS NOT NULL ORDER BY ef."{actual}"')
        return canonical, [str(v) for v in df[actual].dropna().tolist()] if not df.empty else []
    except Exception:
        return canonical, []


def _load_source_names(args):
    db_url, ef_table, src_table, col_map = args
    src_id_col = col_map.get("source_id")
    if not src_id_col or not src_table:
        return "source_name", []
    try:
        src_cols = get_table_columns(db_url, src_table)
        name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols), None)
        if not name_col:
            return "source_name", []
        df = run_query(db_url,
            f'SELECT DISTINCT s."{name_col}" FROM "{src_table}" s '
            f'JOIN "{ef_table}" ef ON ef."{src_id_col}"=s.id '
            f'WHERE s."{name_col}" IS NOT NULL ORDER BY s."{name_col}"')
        return "source_name", df[name_col].dropna().tolist() if not df.empty else []
    except Exception:
        return "source_name", []


@st.cache_data(show_spinner=False, ttl=300)
def load_filter_options(db_url: str, ef_table: str, src_table: str) -> dict:
    col_map = build_column_map(db_url, ef_table)

    filter_cols = [
        "scope", "subcategory", "category_name", "activity_type",
        "lca_activity", "region", "country_code", "year",
        "unit_type", "data_quality", "is_active",
    ]

    join_sql = ""
    src_cols_for_join = get_table_columns(db_url, src_table) if src_table else []
    src_name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols_for_join), None)
    src_id_col = col_map.get("source_id")
    if src_id_col and src_table and src_name_col:
        join_sql = f'LEFT JOIN "{src_table}" s ON ef."{src_id_col}"=s.id'

    # Build tasks
    tasks = [(db_url, ef_table, col_map, c, join_sql, src_table) for c in filter_cols]

    options = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_load_one_filter, t): t[3] for t in tasks}
        futures[executor.submit(_load_source_names, (db_url, ef_table, src_table, col_map))] = "source_name"
        for future in as_completed(futures):
            canonical, vals = future.result()
            options[canonical] = vals

    return options


# ─────────────────────────────────────────
# Query builder using column map
# ─────────────────────────────────────────
def build_where(filters: dict, col_map: dict, src_name_col: str = None) -> tuple:
    parts = []
    params = []

    kw = filters.get("keyword", "").strip()
    if kw:
        searchable_canonicals = ["name", "description", "category_name"]
        searchable = [col_map[c] for c in searchable_canonicals if c in col_map]
        if searchable:
            clauses = " OR ".join(f'LOWER(ef."{c}") LIKE %s' for c in searchable)
            parts.append(f"({clauses})")
            params += [f"%{kw.lower()}%"] * len(searchable)

    if filters.get("active_only", True) and "is_active" in col_map:
        parts.append(f'ef."{col_map["is_active"]}" = TRUE')

    for canonical in ["scope", "subcategory", "category_name", "activity_type",
                       "lca_activity", "region", "country_code", "unit_type", "data_quality"]:
        actual = col_map.get(canonical)
        if not actual:
            continue
        vals = filters.get(canonical, [])
        if vals:
            ph = ", ".join(["%s"] * len(vals))
            parts.append(f'ef."{actual}" IN ({ph})')
            params.extend(vals)

    if "year" in col_map:
        year_vals = filters.get("year", [])
        if year_vals:
            ph = ", ".join(["%s"] * len(year_vals))
            parts.append(f'ef."{col_map["year"]}" IN ({ph})')
            params.extend([int(y) for y in year_vals])

    src_vals = filters.get("source_name", [])
    if src_vals and src_name_col:
        ph = ", ".join(["%s"] * len(src_vals))
        parts.append(f's."{src_name_col}" IN ({ph})')
        params.extend(src_vals)

    is_custom = filters.get("is_custom", "All")
    if "is_custom" in col_map:
        if is_custom == "Custom only":
            parts.append(f'ef."{col_map["is_custom"]}" = TRUE')
        elif is_custom == "Standard only":
            parts.append(f'ef."{col_map["is_custom"]}" = FALSE')

    where_sql = ("WHERE " + " AND ".join(parts)) if parts else ""
    return where_sql, params


def fetch_page(db_url: str, ef_table: str, src_table: str,
               filters: dict, page: int) -> tuple:
    col_map = build_column_map(db_url, ef_table)

    # Build SELECT list — only columns that exist
    sel_parts = []
    for canonical, actual in col_map.items():
        if canonical == actual:
            sel_parts.append(f'ef."{actual}"')
        else:
            sel_parts.append(f'ef."{actual}" AS "{canonical}"')
    ef_col_list = ", ".join(sel_parts) if sel_parts else "ef.*"

    # Source join
    src_cols = get_table_columns(db_url, src_table) if src_table else []
    src_name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols), None)
    src_url_col  = next((c for c in ["url", "link", "website"] if c in src_cols), None)
    src_id_col   = col_map.get("source_id")

    join_sql = ""
    src_select = ""
    if src_id_col and src_table and src_name_col:
        join_sql = f'LEFT JOIN "{src_table}" s ON ef."{src_id_col}"=s.id'
        src_select = f', s."{src_name_col}" AS source_name'
        if src_url_col:
            src_select += f', s."{src_url_col}" AS source_url'

    where_sql, params = build_where(filters, col_map, src_name_col)

    count_df = run_query(db_url,
        f'SELECT COUNT(*) AS cnt FROM "{ef_table}" ef {join_sql} {where_sql}',
        params or None)
    total = int(count_df["cnt"].iloc[0]) if not count_df.empty else 0

    order_parts = [f'ef."{col_map[c]}"' for c in ["scope", "subcategory", "name"] if c in col_map]
    order_sql = ("ORDER BY " + ", ".join(order_parts)) if order_parts else ""

    offset = page * PAGE_SIZE
    data_df = run_query(db_url,
        f'SELECT {ef_col_list}{src_select} FROM "{ef_table}" ef {join_sql} '
        f'{where_sql} {order_sql} LIMIT %s OFFSET %s',
        (params + [PAGE_SIZE, offset]) if params else [PAGE_SIZE, offset])

    if not data_df.empty:
        data_df = data_df.fillna("—")

    return data_df, total, col_map


# ─────────────────────────────────────────
# Sidebar filters
# ─────────────────────────────────────────
def render_filters(options: dict, col_map: dict) -> dict:
    st.sidebar.markdown("""
    <div style="font-size:13px; font-weight:700; color:#1e293b; padding:8px 0 4px 0;
         letter-spacing:0.04em; text-transform:uppercase;">Filters</div>
    """, unsafe_allow_html=True)

    cur = st.session_state.get("active_filters", {})

    # ── Keyword
    keyword = st.sidebar.text_input(
        "🔍 Search",
        value=cur.get("keyword", ""),
        placeholder="name / description / category…",
        key="f_keyword",
    )

    # Only show filters for columns that actually exist in DB
    def multi(label, canonical, fmt_fn=None):
        opts = options.get(canonical, [])
        if not opts or canonical not in col_map:
            return []
        return st.sidebar.multiselect(
            label, opts,
            default=[v for v in cur.get(canonical, []) if v in opts],
            key=f"f_{canonical}",
            format_func=fmt_fn or (lambda x: x),
        )

    st.sidebar.markdown("---")

    scope_sel    = multi("Scope", "scope", lambda x: f"Scope {x}")
    subcat_sel   = multi("Subcategory / Sector", "subcategory")
    cat_sel      = multi("Category", "category_name")
    act_sel      = multi("Activity Type", "activity_type")
    lca_sel      = multi("LCA Activity", "lca_activity")

    st.sidebar.markdown("---")

    region_sel   = multi("Region", "region")
    country_sel  = multi("Country Code", "country_code")
    source_sel   = multi("Source", "source_name") if options.get("source_name") else []
    year_sel     = multi("Year Valid", "year")
    unit_sel     = multi("Unit Type", "unit_type")
    dq_sel       = multi("Data Quality", "data_quality")

    st.sidebar.markdown("---")

    # Custom / Standard radio
    custom_opts  = ["All", "Standard only", "Custom only"]
    cur_custom   = cur.get("is_custom", "All")
    if cur_custom not in custom_opts:
        cur_custom = "All"
    custom_choice = st.sidebar.radio(
        "Factor Type", custom_opts,
        index=custom_opts.index(cur_custom),
        horizontal=True, key="f_custom",
    ) if "is_custom" in col_map else "All"

    active_only = st.sidebar.checkbox(
        "Active records only",
        value=cur.get("active_only", True),
        key="f_active",
    ) if "is_active" in col_map else False

    st.sidebar.markdown("---")
    if st.sidebar.button("🗑 Clear all filters", use_container_width=True):
        keys_to_clear = [k for k in st.session_state if k.startswith("f_")]
        keys_to_clear += ["active_filters", "page_num"]
        for k in keys_to_clear:
            st.session_state.pop(k, None)
        st.rerun()

    return {
        "keyword":       keyword.strip(),
        "scope":         scope_sel,
        "subcategory":   subcat_sel,
        "category_name": cat_sel,
        "activity_type": act_sel,
        "lca_activity":  lca_sel,
        "region":        region_sel,
        "country_code":  country_sel,
        "source_name":   source_sel,
        "year":          year_sel,
        "unit_type":     unit_sel,
        "data_quality":  dq_sel,
        "is_custom":     custom_choice,
        "active_only":   active_only,
    }


# ─────────────────────────────────────────
# Pagination
# ─────────────────────────────────────────
def render_pagination(total: int) -> int:
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if "page_num" not in st.session_state:
        st.session_state["page_num"] = 0
    page = max(0, min(st.session_state["page_num"], total_pages - 1))

    c1, c2, c3 = st.columns([1, 3, 1])
    with c1:
        if st.button("← Prev", disabled=(page == 0), use_container_width=True):
            st.session_state["page_num"] = page - 1
            st.rerun()
    with c2:
        st.markdown(
            f"<div style='text-align:center;padding-top:6px;font-size:13px;color:#64748b'>"
            f"Page {page+1} of {total_pages} &nbsp;·&nbsp; <strong>{total:,}</strong> results</div>",
            unsafe_allow_html=True)
    with c3:
        if st.button("Next →", disabled=(page >= total_pages - 1), use_container_width=True):
            st.session_state["page_num"] = page + 1
            st.rerun()
    return page


# ─────────────────────────────────────────
# Row renderer
# ─────────────────────────────────────────
def _s(row, key):
    v = row.get(key, "—")
    return v if str(v).strip() not in ("", "nan", "None", "—", "NaT") else "—"

def _fmt_factor(val) -> str:
    try:
        f = float(val)
        if f == 0: return "0"
        if abs(f) < 0.0001: return f"{f:.3e}"
        if abs(f) < 1: return f"{f:.6f}".rstrip("0").rstrip(".")
        return f"{f:.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(val)

def _confidence_html(score) -> str:
    try:
        pct = int(float(score) * 100)
        return (f'<div class="quality-bar-wrap"><div class="quality-bar" style="width:{pct}%"></div></div>{pct}%')
    except Exception:
        return str(score)

def render_table_header():
    h = st.columns([3.2, 0.9, 1.4, 1.3, 0.7, 1.0])
    for col, label in zip(h, ["ACTIVITY NAME", "SCOPE", "CO₂e FACTOR", "SOURCE", "YEAR", "REGION"]):
        col.markdown(f'<div class="col-header">{label}</div>', unsafe_allow_html=True)

def render_row(row, idx):
    name          = _s(row, "name")
    scope         = _s(row, "scope")
    subcategory   = _s(row, "subcategory")
    category_name = _s(row, "category_name")
    co2e          = _s(row, "co2e_factor")
    co2           = _s(row, "co2_factor")
    ch4           = _s(row, "ch4_factor")
    n2o           = _s(row, "n2o_factor")
    bio           = _s(row, "biogenic_co2_factor")
    source_name   = _s(row, "source_name")
    source_url    = _s(row, "source_url")
    source_ref    = _s(row, "source_reference")
    year          = _s(row, "year")
    year_released = _s(row, "year_released")
    region        = _s(row, "region")
    region_name   = _s(row, "region_name")
    country_code  = _s(row, "country_code")
    activity_type = _s(row, "activity_type")
    lca           = _s(row, "lca_activity")
    description   = _s(row, "description")
    unit_type     = _s(row, "unit_type")
    factor_unit   = _s(row, "factor_unit")
    input_unit    = _s(row, "input_unit")
    confidence    = _s(row, "confidence_score")
    data_quality  = _s(row, "data_quality")
    uid           = _s(row, "id")
    climatiq_id   = _s(row, "climatiq_id")
    is_active     = str(_s(row, "is_active")).lower()
    is_custom     = str(_s(row, "is_custom")).lower()
    tags          = _s(row, "tags")
    created_at    = _s(row, "created_at")
    updated_at    = _s(row, "updated_at")

    scope_str = str(scope).strip()
    scope_valid = scope_str not in ("", "—", "nan", "None")
    scope_class = f"scope-{scope_str}" if scope_str in ("1","2","3") else "scope-other"
    # Row badge: only the numeric scope (short), no subcategory to avoid overflow
    scope_badge_label = f"Scope {scope_str}" if scope_valid else (subcategory[:18] + "…" if subcategory != "—" and len(subcategory) > 18 else subcategory if subcategory != "—" else "—")
    # Full label used inside expanded detail
    if scope_valid and subcategory != "—":
        scope_full_label = f"Scope {scope_str} · {subcategory}"
    elif scope_valid:
        scope_full_label = f"Scope {scope_str}"
    elif subcategory != "—":
        scope_full_label = subcategory
    else:
        scope_full_label = "—"
    co2e_display = f"{_fmt_factor(co2e)} {factor_unit}" if co2e != "—" else "—"
    active_class = "status-active" if is_active == "true" else "status-inactive"
    active_label = "Active" if is_active == "true" else "Inactive"
    custom_label = "✅ Custom" if is_custom == "true" else "Standard"

    if source_url != "—":
        source_html = f'<a href="{source_url}" target="_blank" class="kv-link">{source_name}</a>'
    else:
        source_html = f'<span>{source_name}</span>'

    cols = st.columns([3.2, 0.9, 1.4, 1.3, 0.7, 1.0])
    cols[1].markdown(f'<span class="scope-badge {scope_class}">{scope_badge_label}</span>',
                     unsafe_allow_html=True)
    cols[2].markdown(f"<small style='color:#2563eb;font-weight:600;font-family:IBM Plex Mono,monospace'>{co2e_display}</small>",
                     unsafe_allow_html=True)
    cols[3].markdown(f"<small style='color:#475569'>{source_name}</small>", unsafe_allow_html=True)
    cols[4].markdown(f"<small style='color:#475569'>{year}</small>", unsafe_allow_html=True)
    cols[5].markdown(f"<small style='color:#475569'>{region}</small>", unsafe_allow_html=True)

    with cols[0]:
        display_name = re.sub(r"\[.*?\]", "", name).strip() or name
        with st.expander(f"▸  {display_name}"):
            if category_name != "—":
                st.markdown(f'<div class="lca-sub">Category: <strong>{category_name}</strong></div>', unsafe_allow_html=True)
            if lca != "—":
                st.markdown(f'<div class="lca-sub">LCA Activity: <code>{lca}</code></div>', unsafe_allow_html=True)
            if description != "—":
                st.markdown(f'<p class="desc-block">{description}</p>', unsafe_allow_html=True)

            pills = []
            if co2e != "—": pills.append(("CO₂e", _fmt_factor(co2e), factor_unit))
            for label, col_key in [("CO₂", co2), ("CH₄", ch4), ("N₂O", n2o), ("Biogenic CO₂", bio)]:
                if col_key != "—":
                    try:
                        if float(col_key) > 0:
                            pills.append((label, _fmt_factor(col_key), factor_unit))
                    except Exception:
                        pass

            if pills:
                pills_html = "".join(
                    f'<div class="ef-pill">{l}: <span>{v}</span> {u}</div>'
                    for l, v, u in pills)
                st.markdown(f'<div class="ef-grid">{pills_html}</div>', unsafe_allow_html=True)

            st.markdown(f"""
<table class="kv-table">
  <tr><td>SOURCE</td><td>{source_html}</td></tr>
  <tr><td>SOURCE REFERENCE</td><td>{source_ref}</td></tr>
  <tr><td>YEAR VALID</td><td>{year}</td></tr>
  <tr><td>YEAR RELEASED</td><td>{year_released}</td></tr>
  <tr><td>REGION</td><td>{region}{(" — " + region_name) if region_name != "—" else ""}</td></tr>
  <tr><td>COUNTRY CODE</td><td>{country_code}</td></tr>
  <tr><td>UNIT TYPE</td><td>{unit_type}</td></tr>
  <tr><td>INPUT UNIT</td><td>{input_unit}</td></tr>
  <tr><td>FACTOR UNIT</td><td>{factor_unit}</td></tr>
  <tr><td>ACTIVITY TYPE</td><td>{activity_type}</td></tr>
  <tr><td>STATUS</td><td><span class="{active_class}">{active_label}</span>&nbsp;&nbsp;{custom_label}</td></tr>
  <tr><td>CONFIDENCE SCORE</td><td>{_confidence_html(confidence)}</td></tr>
  <tr><td>DATA QUALITY</td><td>{data_quality}</td></tr>
</table>""", unsafe_allow_html=True)

            st.markdown('<span class="meta-header">IDENTIFIERS & METADATA</span>', unsafe_allow_html=True)
            lca_display  = f'<span class="id-chip">{lca}</span>' if lca != "—" else "—"
            tags_display = tags if tags != "—" else "—"
            uid_display  = f'<span class="id-chip">{uid}</span>'
            cid_display  = f'<span class="id-chip">{climatiq_id}</span>'
            cat_display  = category_name if category_name != "—" else "—"
            ts_style     = "font-family:'IBM Plex Mono',monospace;font-size:11.5px;color:#475569"
            st.markdown(f"""
<table class="kv-table">
  <tr><td>ID (UUID)</td><td>{uid_display}</td></tr>
  <tr><td>CLIMATIQ ID</td><td>{cid_display}</td></tr>
  <tr><td>SCOPE</td><td>{scope_full_label}</td></tr>
  <tr><td>CATEGORY</td><td>{cat_display}</td></tr>
  <tr><td>LCA ACTIVITY</td><td>{lca_display}</td></tr>
  <tr><td>TAGS</td><td>{tags_display}</td></tr>
  <tr><td>CREATED AT</td><td><span style="{ts_style}">{created_at}</span></td></tr>
  <tr><td>UPDATED AT</td><td><span style="{ts_style}">{updated_at}</span></td></tr>
</table>""", unsafe_allow_html=True)

    st.markdown("<hr class='row-sep'>", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Table picker
# ─────────────────────────────────────────
def render_table_picker(db_url: str):
    all_tables = discover_tables(db_url)
    ef_auto    = find_best_candidate(all_tables, EF_TABLE_CANDIDATES)
    src_auto   = find_best_candidate(all_tables, SRC_TABLE_CANDIDATES)

    ef_choices  = sorted(set(
        [t for t in all_tables if any(k in t.lower() for k in ["emission", "factor"])]
    )) or all_tables
    src_choices = sorted(set(
        [t for t in all_tables if any(k in t.lower() for k in ["source", "factor"])]
    )) or all_tables

    with st.expander("⚙️ Table Configuration", expanded=(ef_auto is None)):
        if ef_auto:
            st.success(f"Auto-detected emission factor table: **{ef_auto}**")
        else:
            st.warning("Could not auto-detect the emission factor table. Please select below.")

        col1, col2 = st.columns(2)
        with col1:
            ef_options = list(dict.fromkeys(([ef_auto] if ef_auto else []) + ef_choices + all_tables))
            ef_table = st.selectbox("Emission Factor table", options=ef_options, index=0, key="tbl_ef")
        with col2:
            src_options = list(dict.fromkeys(([src_auto] if src_auto else [""]) + [""] + src_choices + all_tables))
            src_table = st.selectbox("Factor Source table (optional)", options=src_options, index=0,
                                     key="tbl_src", help="Leave blank if no separate source table.")

        if st.button("✅ Use these tables", type="primary"):
            st.session_state["ef_table"]  = ef_table
            st.session_state["src_table"] = src_table or ""
            for k in ["active_filters", "page_num"]:
                st.session_state.pop(k, None)
            load_filter_options.clear()
            get_table_columns.clear()
            build_column_map.clear()
            st.rerun()

    # Show detected column mapping (collapsible)
    ef_tbl = st.session_state.get("ef_table", ef_auto or "")
    if ef_tbl:
        col_map = build_column_map(db_url, ef_tbl)
        missing = [c for c in COLUMN_ALIASES if c not in col_map]
        if missing:
            with st.expander(f"ℹ️ Column mapping — {len(col_map)} matched, {len(missing)} not found"):
                st.markdown("**Matched columns:**")
                for canon, actual in col_map.items():
                    label = f"`{canon}`" if canon == actual else f"`{canon}` → `{actual}`"
                    st.markdown(f"- {label}")
                st.markdown("**Not found (will be skipped):**")
                st.markdown(", ".join(f"`{c}`" for c in missing))

    return (
        st.session_state.get("ef_table",  ef_auto or ""),
        st.session_state.get("src_table", src_auto or ""),
    )


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
def main():
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

    # ── Auto-connect ─────────────────────────────────────────────────────────
    HARDCODED_URL = "postgresql://emission_user:CarbonPassword2026@emission-pg.postgres.database.azure.com:5432/emission_db?ssl=require"
    if HARDCODED_URL and "db_url" not in st.session_state:
        try:
            conn = get_connection(HARDCODED_URL)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            st.session_state["db_url"] = HARDCODED_URL
        except Exception as e:
            st.error(f"Auto-connect failed: {e}")

    # ── Connection panel ──────────────────────────────────────────────────────
    with st.expander("🔌 Database Connection",
                     expanded=("db_url" not in st.session_state)):
        if "db_url" in st.session_state:
            st.success("✅ Connected.")
            st.caption("To use a different database, enter a new URL and click Connect.")
        else:
            st.caption("Enter your PostgreSQL connection URL. Session is read-only.")

        db_url_input = st.text_input(
            "Connection URL", value="",
            placeholder="postgresql://user:password@host:port/dbname",
            type="password", label_visibility="collapsed", key="db_url_input")

        if st.button("Connect", type="primary"):
            url = db_url_input.strip()
            if not url:
                st.error("Please enter a connection URL.")
            else:
                try:
                    conn = get_connection(url)
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                    st.session_state["db_url"] = url
                    for k in ["ef_table", "src_table", "active_filters", "page_num"]:
                        st.session_state.pop(k, None)
                    st.rerun()
                except Exception as e:
                    st.error(f"Connection failed: {e}")
                    return

    if "db_url" not in st.session_state:
        st.info("Enter your database URL above to begin.")
        return

    db_url = st.session_state["db_url"]

    # ── Table picker ──────────────────────────────────────────────────────────
    ef_table, src_table = render_table_picker(db_url)
    if not ef_table:
        st.warning("No emission factor table found. Run `python manage.py migrate` then refresh.")
        all_tables = discover_tables(db_url)
        if all_tables:
            with st.expander("📋 Tables in database"):
                st.write(all_tables)
        return

    # ── Column map ────────────────────────────────────────────────────────────
    col_map = build_column_map(db_url, ef_table)

    # ── Filter options (parallel, cached) ────────────────────────────────────
    with st.spinner("Loading filters…"):
        try:
            options = load_filter_options(db_url, ef_table, src_table)
        except Exception as e:
            st.error(f"Could not load filter options: {e}")
            return

    active = render_filters(options, col_map)
    st.session_state["active_filters"] = active

    # ── Active filter summary ─────────────────────────────────────────────────
    active_tags = []
    for k, v in active.items():
        if k in {"active_only", "is_custom"}: continue
        if v and v not in ([], ""):
            label = k.replace("_", " ").title()
            active_tags.append(f"**{label}:** {', '.join(str(x) for x in v) if isinstance(v, list) else v}")
    if active.get("active_only"): active_tags.append("**Status:** Active only")
    if active.get("is_custom", "All") != "All": active_tags.append(f"**Type:** {active['is_custom']}")
    if active_tags:
        st.info("🔎 " + " | ".join(active_tags))

    # Reset page on filter change
    filter_key = str(sorted(str(active.items())))
    if st.session_state.get("_last_filter_key") != filter_key:
        st.session_state["page_num"] = 0
        st.session_state["_last_filter_key"] = filter_key

    # ── Fetch & render ────────────────────────────────────────────────────────
    page = st.session_state.get("page_num", 0)

    with st.spinner("Querying…"):
        try:
            page_df, total, col_map = fetch_page(db_url, ef_table, src_table, active, page)
        except Exception as e:
            st.error(f"Query error: {e}")
            st.exception(e)
            return

    if total == 0:
        st.warning("No results match your filters.")
        return

    render_pagination(total)
    render_table_header()
    for _, row in page_df.iterrows():
        render_row(row, _)
    render_pagination(total)


if __name__ == "__main__":
    main()