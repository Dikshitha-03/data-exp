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

    /* ── Column headers ── */
    .col-header {
        font-size: 10px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.10em; color: #94a3b8;
        padding-bottom: 8px; border-bottom: 1px solid #e2e8f0; margin-bottom: 6px;
    }

    /* ── Scope badges ── */
    .scope-badge {
        display: inline-flex; align-items: center; padding: 3px 10px;
        border-radius: 20px; font-size: 11px; font-weight: 700;
        white-space: nowrap; letter-spacing: 0.04em;
    }
    .scope-1 { background:#dcfce7; color:#14532d; border: 1px solid #bbf7d0; }
    .scope-2 { background:#dbeafe; color:#1e3a8a; border: 1px solid #bfdbfe; }
    .scope-3 { background:#fce7f3; color:#831843; border: 1px solid #fbcfe8; }
    .scope-other { background:#f1f5f9; color:#475569; border: 1px solid #e2e8f0; }

    /* ── EF Row Card ── */
    .ef-row-card {
        background: #ffffff;
        border: 1px solid #e8ecf2;
        border-radius: 10px;
        margin-bottom: 6px;
        overflow: hidden;
        transition: box-shadow 0.18s ease, border-color 0.18s ease;
    }
    .ef-row-card:hover {
        box-shadow: 0 2px 12px rgba(15,23,42,0.07);
        border-color: #cbd5e1;
    }

    /* ── Row summary bar ── */
    .ef-summary {
        display: grid;
        grid-template-columns: 3.2fr 0.9fr 1.4fr 1.3fr 0.7fr 1fr 32px;
        align-items: center;
        gap: 0;
        padding: 12px 14px 12px 16px;
        cursor: pointer;
        user-select: none;
    }
    .ef-summary:hover { background: #f8fafc; }

    .ef-name {
        font-size: 13.5px; font-weight: 500; color: #1e293b;
        line-height: 1.35; padding-right: 12px;
    }
    .ef-name small { display:block; font-size:11px; color:#94a3b8; font-weight:400; margin-top:1px; }

    .ef-co2 {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 12.5px; font-weight: 600; color: #2563eb;
    }
    .ef-meta { font-size: 12.5px; color: #64748b; }

    .ef-chevron {
        width: 24px; height: 24px; border-radius: 6px;
        display: flex; align-items: center; justify-content: center;
        color: #94a3b8; font-size: 13px;
        transition: transform 0.2s ease, background 0.15s ease;
        flex-shrink: 0;
    }
    .ef-row-card.open .ef-chevron { transform: rotate(180deg); background: #f1f5f9; color: #475569; }

    /* ── Expanded detail panel ── */
    .ef-detail {
        border-top: 1px solid #f1f5f9;
        background: #fafbfc;
        display: none;
        padding: 16px 18px 18px 18px;
        animation: slideDown 0.18s ease;
    }
    .ef-row-card.open .ef-detail { display: block; }

    @keyframes slideDown {
        from { opacity: 0; transform: translateY(-6px); }
        to   { opacity: 1; transform: translateY(0); }
    }

    /* ── Detail two-column grid ── */
    .detail-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 20px;
        margin-top: 4px;
    }

    /* ── Description ── */
    .desc-block {
        font-size: 12.5px; color: #475569; line-height: 1.7;
        background: #f0f4f8; border: 1px solid #e2e8f0; border-radius: 8px;
        padding: 10px 13px; margin-bottom: 14px;
        border-left: 3px solid #93c5fd;
    }

    /* ── EF pills ── */
    .ef-pills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 14px; }
    .ef-pill {
        background: #eff6ff; border: 1px solid #bae6fd;
        border-radius: 20px; padding: 4px 12px;
        font-size: 11.5px; color: #0369a1;
    }
    .ef-pill .val { font-weight: 700; color: #0c4a6e; font-family: 'IBM Plex Mono', monospace; }

    /* ── Section label ── */
    .section-label {
        font-size: 10px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.10em; color: #94a3b8;
        margin: 0 0 8px 0; display: block;
    }

    /* ── KV table ── */
    .kv-table {
        width: 100%; border-collapse: collapse; font-size: 12.5px;
        border: 1px solid #e8ecf2; border-radius: 8px;
        overflow: hidden; margin-bottom: 14px;
        background: #ffffff;
    }
    .kv-table tr { border-bottom: 1px solid #f1f5f9; }
    .kv-table tr:last-child { border-bottom: none; }
    .kv-table td { padding: 7px 12px; vertical-align: middle; }
    .kv-table td:first-child {
        font-size: 10px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.06em; color: #64748b;
        width: 130px; min-width: 130px; white-space: nowrap;
        background: #f8fafc; border-right: 1px solid #f1f5f9;
    }
    .kv-table td:last-child {
        color: #1e293b; font-size: 13px; word-break: break-word;
    }
    .kv-link { color: #2563eb; font-weight: 500; text-decoration: none; }
    .kv-link:hover { text-decoration: underline; }

    /* ── Chips ── */
    .id-chip {
        background: #f1f5f9; border-radius: 4px; padding: 2px 6px;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 10.5px; color: #334155; display: inline-block;
        word-break: break-all; border: 1px solid #e2e8f0;
    }

    /* ── Status ── */
    .status-active   { font-weight: 700; color: #16a34a; }
    .status-inactive { font-weight: 700; color: #dc2626; }
    .license-badge {
        display: inline-block; padding: 1px 8px; border-radius: 12px;
        font-size: 10px; font-weight: 600; margin-left: 6px;
        background: #f1f5f9; color: #475569;
        border: 1px solid #e2e8f0; vertical-align: middle;
    }

    /* ── Confidence bar ── */
    .conf-wrap { display: inline-flex; align-items: center; gap: 7px; }
    .conf-bar-bg {
        width: 60px; height: 4px; background: #e2e8f0;
        border-radius: 2px; overflow: hidden; display: inline-block;
    }
    .conf-bar { height: 4px; background: #3b82f6; border-radius: 2px; }

    /* ── Timestamps ── */
    .ts-mono { font-family:'IBM Plex Mono',monospace; font-size:11.5px; color:#94a3b8; }

    /* ── Filter tag summary ── */
    .filter-tag {
        background:#eff6ff; border:1px solid #bfdbfe; border-radius:4px;
        padding:2px 8px; font-size:11px; color:#1d4ed8;
        display:inline-block; margin:2px;
    }
</style>

<script>
function toggleRow(id) {
    var card = document.getElementById('efcard-' + id);
    if (card) { card.classList.toggle('open'); }
}
</script>
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

COLUMN_ALIASES = {
    "id":                   ["id", "uuid", "pk"],
    "climatiq_id":          ["climatiq_id", "activity_id", "external_id", "factor_id"],
    "external_ref":         ["external_ref", "external_reference", "ref"],
    "scope":                ["scope", "ghg_scope", "emission_scope", "ghg_category", "scope_category", "scope_name"],
    "sector":               ["sector", "industry_sector", "economic_sector"],
    "subcategory":          ["subcategory", "sub_category", "category_sector", "sector_detail", "sector"],
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
        if df.empty:
            return canonical, []
        seen = set()
        vals = []
        for v in df[actual].dropna():
            s = str(v).strip()
            if s and s.lower() not in seen:
                seen.add(s.lower())
                vals.append(s)
        return canonical, sorted(vals)
    except Exception:
        return canonical, []


def _load_scope_options(args):
    db_url, ef_table = args
    candidates = ["scope", "ghg_scope", "emission_scope", "scope_category",
                  "scope_name", "ghg_category", "subcategory", "sub_category", "sector"]
    try:
        df_cols = run_query(db_url,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s", (ef_table,))
        actual_cols = set(df_cols["column_name"].tolist()) if not df_cols.empty else set()
        for col in candidates:
            if col not in actual_cols:
                continue
            try:
                df = run_query(db_url,
                    f'SELECT DISTINCT "{col}" FROM "{ef_table}" '
                    f'WHERE "{col}" IS NOT NULL ORDER BY "{col}"')
                if not df.empty:
                    seen = set()
                    vals = []
                    for v in df[col].dropna():
                        s = str(v).strip()
                        if s and s.lower() not in seen:
                            seen.add(s.lower())
                            vals.append(s)
                    return "scope", sorted(vals)
            except Exception:
                continue
    except Exception:
        pass
    return "scope", []


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
        if df.empty:
            return "source_name", []
        seen = set()
        vals = []
        for v in df[name_col].dropna():
            s = str(v).strip()
            if s and s.lower() not in seen:
                seen.add(s.lower())
                vals.append(s)
        return "source_name", sorted(vals)
    except Exception:
        return "source_name", []


@st.cache_data(show_spinner=False, ttl=300)
def load_filter_options(db_url: str, ef_table: str, src_table: str) -> dict:
    col_map = build_column_map(db_url, ef_table)

    filter_cols = [
        "sector", "subcategory", "category_name", "activity_type",
        "lca_activity", "region", "country_code", "year", "year_released",
        "unit_type", "data_quality", "is_active",
    ]

    join_sql = ""
    src_cols_for_join = get_table_columns(db_url, src_table) if src_table else []
    src_name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols_for_join), None)
    src_id_col = col_map.get("source_id")
    if src_id_col and src_table and src_name_col:
        join_sql = f'LEFT JOIN "{src_table}" s ON ef."{src_id_col}"=s.id'

    tasks = [(db_url, ef_table, col_map, c, join_sql, src_table) for c in filter_cols]

    options = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_load_one_filter, t): t[3] for t in tasks}
        futures[executor.submit(_load_source_names, (db_url, ef_table, src_table, col_map))] = "source_name"
        futures[executor.submit(_load_scope_options, (db_url, ef_table))] = "scope"
        for future in as_completed(futures):
            canonical, vals = future.result()
            options[canonical] = vals

    return options


# ─────────────────────────────────────────
# Query builder
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

    for canonical in ["sector", "subcategory", "category_name", "activity_type",
                       "lca_activity", "region", "country_code", "unit_type", "data_quality"]:
        actual = col_map.get(canonical)
        if not actual:
            continue
        vals = filters.get(canonical, [])
        if vals:
            ph = ", ".join(["%s"] * len(vals))
            parts.append(f'ef."{actual}" IN ({ph})')
            params.extend(vals)

    scope_vals = filters.get("scope", [])
    if scope_vals:
        scope_actual = next(
            (col_map.get(c) for c in ["scope", "ghg_scope", "emission_scope",
             "scope_category", "scope_name", "ghg_category", "subcategory", "sector"]
             if col_map.get(c)),
            None
        )
        if scope_actual:
            ph = ", ".join(["%s"] * len(scope_vals))
            parts.append(f'ef."{scope_actual}" IN ({ph})')
            params.extend(scope_vals)

    if "year" in col_map:
        year_vals = filters.get("year", [])
        if year_vals:
            ph = ", ".join(["%s"] * len(year_vals))
            parts.append(f'ef."{col_map["year"]}" IN ({ph})')
            params.extend([int(y) for y in year_vals])

    if "year_released" in col_map:
        yr_vals = filters.get("year_released", [])
        if yr_vals:
            ph = ", ".join(["%s"] * len(yr_vals))
            parts.append(f'ef."{col_map["year_released"]}" IN ({ph})')
            params.extend([int(y) for y in yr_vals])

    src_vals = filters.get("source_name", [])
    if src_vals and src_name_col:
        ph = ", ".join(["%s"] * len(src_vals))
        parts.append(f's."{src_name_col}" IN ({ph})')
        params.extend(src_vals)

    license_val = filters.get("license", "All")
    if "is_custom" in col_map:
        if license_val == "Core":
            parts.append(f'ef."{col_map["is_custom"]}" = FALSE')
        elif license_val == "Premium":
            parts.append(f'ef."{col_map["is_custom"]}" = TRUE')

    where_sql = ("WHERE " + " AND ".join(parts)) if parts else ""
    return where_sql, params


def fetch_page(db_url: str, ef_table: str, src_table: str,
               filters: dict, page: int) -> tuple:
    col_map = build_column_map(db_url, ef_table)

    sel_parts = []
    for canonical, actual in col_map.items():
        if canonical == actual:
            sel_parts.append(f'ef."{actual}"')
        else:
            sel_parts.append(f'ef."{actual}" AS "{canonical}"')
    ef_col_list = ", ".join(sel_parts) if sel_parts else "ef.*"

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

    keyword = st.sidebar.text_input(
        "Keywords", value=cur.get("keyword", ""),
        placeholder="Search…", key="f_keyword",
    )

    def multi(label, canonical, fmt_fn=None):
        opts = options.get(canonical, [])
        if canonical not in col_map:
            return []
        if not opts:
            return []
        return st.sidebar.multiselect(
            label, opts,
            default=[v for v in cur.get(canonical, []) if v in opts],
            key=f"f_{canonical}",
            format_func=fmt_fn or (lambda x: x),
        )

    def multi_free(label, key, opts):
        if not opts:
            return []
        return st.sidebar.multiselect(
            label, opts,
            default=[v for v in cur.get(key, []) if v in opts],
            key=f"f_{key}",
        )

    _scope_canonical = "scope" if "scope" in col_map else ("subcategory" if "subcategory" in col_map else None)
    if _scope_canonical:
        _scope_opts = options.get("scope") or options.get("subcategory") or []
        if _scope_opts:
            scope_sel = st.sidebar.multiselect(
                "Scope", _scope_opts,
                default=[v for v in cur.get("scope", []) if v in _scope_opts],
                key="f_scope",
            )
        else:
            scope_sel = []
    else:
        scope_sel = []

    sector_sel   = multi("Sector (Industry)", "sector")
    subcat_sel   = multi("Subcategory", "subcategory")
    cat_sel      = multi("Category", "category_name")

    st.sidebar.markdown("---")

    region_sel   = multi("Region", "region")
    source_sel   = multi_free("Source", "source_name", options.get("source_name", [])) if options.get("source_name") else []
    year_sel     = multi("Year Valid", "year")
    unit_sel     = multi("Unit Type", "unit_type")
    lca_sel      = multi("Lifecycle Activity", "lca_activity")

    license_opts = ["All", "Core", "Premium"]
    cur_license  = cur.get("license", "All")
    if cur_license not in license_opts:
        cur_license = "All"
    if "is_custom" in col_map:
        st.sidebar.markdown(
            '<div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:4px;">License</div>',
            unsafe_allow_html=True)
        license_choice = st.sidebar.radio(
            "License", license_opts,
            index=license_opts.index(cur_license),
            horizontal=True, key="f_license",
            label_visibility="collapsed",
        )
    else:
        license_choice = "All"

    data_version_sel = multi("Data Version", "year_released")

    st.sidebar.markdown("---")

    act_sel     = multi("Activity Type", "activity_type")
    country_sel = multi("Country Code", "country_code")
    dq_sel      = multi("Data Quality", "data_quality")

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
        "keyword":        keyword.strip(),
        "scope":          scope_sel,
        "sector":         sector_sel,
        "subcategory":    subcat_sel,
        "category_name":  cat_sel,
        "activity_type":  act_sel,
        "lca_activity":   lca_sel,
        "region":         region_sel,
        "country_code":   country_sel,
        "source_name":    source_sel,
        "year":           year_sel,
        "year_released":  data_version_sel,
        "unit_type":      unit_sel,
        "data_quality":   dq_sel,
        "license":        license_choice,
        "active_only":    active_only,
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
# Row renderer helpers
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
        return (
            f'<span class="conf-wrap">'
            f'<span class="conf-bar-bg"><span class="conf-bar" style="width:{pct}%"></span></span>'
            f'<span style="font-size:12px;color:#475569">{pct}%</span></span>'
        )
    except Exception:
        return str(score)


# ─────────────────────────────────────────
# Table header
# ─────────────────────────────────────────
def render_table_header():
    st.markdown("""
    <div style="display:grid;grid-template-columns:3.2fr 0.9fr 1.5fr 1fr 1fr 0.5fr 32px;
         gap:0;padding:0 14px 8px 16px;margin-top:4px;">
        <div class="col-header">ACTIVITY NAME</div>
        <div class="col-header">SCOPE</div>
        <div class="col-header">CO₂E FACTOR</div>
        <div class="col-header">SOURCE</div>
        <div class="col-header">YEAR</div>
        <div class="col-header">REGION</div>
        <div></div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────
# Row renderer — custom card accordion
# ─────────────────────────────────────────
# ── Call this ONCE at the top of your page, before any render_row() calls ──
def render_table_header():
    """Renders the sticky column header row."""
    cols = st.columns([3, 1, 1.2, 1.2, 0.7, 1])
    headers = ["ACTIVITY NAME", "SCOPE", "CO₂E FACTOR", "SOURCE", "YEAR", "REGION"]
    for col, h in zip(cols, headers):
        col.markdown(
            f"<div style='font-size:10px;font-weight:700;letter-spacing:0.1em;"
            f"color:#94a3b8;padding-bottom:6px;border-bottom:2px solid #e2e8f0;'>"
            f"{h}</div>",
            unsafe_allow_html=True
        )
    st.markdown("<div style='margin-bottom:4px;'></div>", unsafe_allow_html=True)


# ── Call this for every row ──
def render_row(row, idx):

    def g(key):
        v = row.get(key, "—")
        return v if str(v).strip() not in ("", "nan", "None", "—", "NaT") else "—"

    name        = g("name")
    scope       = g("scope")
    category    = g("category_name")
    subcategory = g("subcategory")
    co2e        = g("co2e_factor")
    source      = g("source_name")
    year        = g("year")
    region      = g("region")
    unit        = g("factor_unit")
    input_unit  = g("input_unit")
    desc        = g("description")
    activity    = g("activity_type")
    lca         = g("lca_activity")
    confidence  = g("confidence_score")
    quality     = g("data_quality")
    uid         = g("id")
    climatiq    = g("climatiq_id")

    co2e_display = f"{_fmt_factor(co2e)} {unit}" if co2e != "—" else "—"

    # Scope badge colors
    scope_colors = {
        "1": ("#fff3e0", "#e65100"),
        "2": ("#e8f5e9", "#2e7d32"),
        "3": ("#e3f2fd", "#1565c0"),
    }
    badge_bg, badge_fg = scope_colors.get(str(scope), ("#f3e5f5", "#6a1b9a"))

    # ── Session state toggle ──
    exp_key = f"exp_{idx}"
    if exp_key not in st.session_state:
        st.session_state[exp_key] = False

    # ── Row — same column widths as header ──
    cols = st.columns([3, 1, 1.2, 1.2, 0.7, 1])

    with cols[0]:
        arrow = "▼" if st.session_state[exp_key] else "▶"
        # Inline button styled to look like plain text
        st.markdown(
            f"""<style>
            div[data-testid="stButton"] button#btn_{idx} {{
                background:none;border:none;padding:0;
                font-size:14px;font-weight:600;color:#1e293b;
                text-align:left;cursor:pointer;
            }}
            div[data-testid="stButton"] button#btn_{idx}:hover {{
                color:#6366f1;
            }}
            </style>""",
            unsafe_allow_html=True
        )
        if st.button(f"{arrow}  {name}", key=f"btn_{idx}", use_container_width=True):
            st.session_state[exp_key] = not st.session_state[exp_key]

    cols[1].markdown(
        f"<span style='background:{badge_bg};color:{badge_fg};"
        f"padding:3px 9px;border-radius:20px;font-size:11px;"
        f"font-weight:700;white-space:nowrap;'>Scope {scope}</span>",
        unsafe_allow_html=True
    )
    cols[2].markdown(
        f"<span style='font-family:monospace;font-size:13px;"
        f"font-weight:600;color:#166534;background:#f0fdf4;"
        f"padding:3px 8px;border-radius:5px;'>{co2e_display}</span>",
        unsafe_allow_html=True
    )
    cols[3].markdown(f"<span style='font-size:13px;color:#475569;'>{source}</span>", unsafe_allow_html=True)
    cols[4].markdown(f"<span style='font-size:13px;color:#475569;'>{year}</span>",   unsafe_allow_html=True)
    cols[5].markdown(f"<span style='font-size:13px;color:#475569;'>{region}</span>", unsafe_allow_html=True)

    # ── Expandable detail panel ──
    if st.session_state[exp_key]:

        def field(label, value):
            color = "#94a3b8" if value == "—" else "#1e293b"
            extra = "font-style:italic;" if value == "—" else "font-weight:600;"
            st.markdown(
                f"""<div style="margin-bottom:10px;">
                    <div style="font-size:10px;font-weight:700;letter-spacing:0.07em;
                                text-transform:uppercase;color:#94a3b8;margin-bottom:3px;">
                        {label}
                    </div>
                    <div style="background:#f1f5f9;border:1px solid #e2e8f0;
                                padding:5px 11px;border-radius:6px;font-size:13px;
                                color:{color};{extra}word-break:break-word;">
                        {value}
                    </div>
                </div>""",
                unsafe_allow_html=True
            )

        def section_header(text, color, border_color):
            st.markdown(
                f"<div style='font-size:11px;font-weight:700;color:{color};"
                f"text-transform:uppercase;letter-spacing:0.07em;"
                f"border-bottom:2px solid {border_color};"
                f"padding-bottom:5px;margin-bottom:10px;'>{text}</div>",
                unsafe_allow_html=True
            )

        with st.container():
            st.markdown(
                "<div style='background:#f8fafc;border:1px solid #e2e8f0;"
                "border-radius:10px;padding:16px;margin:6px 0 8px 0;'>",
                unsafe_allow_html=True
            )

            if desc != "—":
                st.markdown(
                    f"<div style='background:#eef2ff;border-left:4px solid #6366f1;"
                    f"padding:10px 14px;border-radius:6px;font-size:14px;"
                    f"color:#334155;line-height:1.6;margin-bottom:12px;'>"
                    f"{desc}</div>",
                    unsafe_allow_html=True
                )

            c1, c2, c3 = st.columns(3)

            with c1:
                section_header(" Emission Info", "#6366f1", "#e0e7ff")
                field("CO₂e Factor", co2e_display)
                field("Unit", unit)
                field("Input Unit", input_unit)
                field("Source", source)
                field("Year", year)
                field("Region", region)

            with c2:
                section_header(" Classification", "#0891b2", "#cffafe")
                field("Category", category)
                field("Subcategory", subcategory)
                field("Activity", activity)
                field("LCA", lca)
                field("Confidence", confidence)
                field("Data Quality", quality)

            with c3:
                section_header(" IDs", "#7c3aed", "#ede9fe")
                field("UUID", uid)
                field("Climatiq ID", climatiq)

            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        "<hr style='margin:4px 0;border:none;border-top:1px solid #f1f5f9;'>",
        unsafe_allow_html=True
    )
# ─────────────────────────────────────────
# Silent table auto-detection
# ─────────────────────────────────────────
def resolve_tables(db_url: str) -> tuple:
    if "ef_table" in st.session_state and "src_table" in st.session_state:
        return st.session_state["ef_table"], st.session_state["src_table"]

    all_tables = discover_tables(db_url)
    ef_table  = find_best_candidate(all_tables, EF_TABLE_CANDIDATES) or ""
    src_table = find_best_candidate(all_tables, SRC_TABLE_CANDIDATES) or ""

    st.session_state["ef_table"]  = ef_table
    st.session_state["src_table"] = src_table
    return ef_table, src_table


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

    # ── Silent auto-connect ──
    HARDCODED_URL = "postgresql://emission_user:CarbonPassword2026@emission-pg.postgres.database.azure.com:5432/emission_db?ssl=require"
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
    SKIP_SUMMARY = {"active_only", "license"}
    LABEL_MAP = {
        "keyword": "Keywords", "scope": "Scope", "sector": "Sector",
        "subcategory": "Subcategory", "category_name": "Category",
        "activity_type": "Activity Type", "lca_activity": "Lifecycle Activity",
        "region": "Region", "country_code": "Country", "source_name": "Source",
        "year": "Year Valid", "year_released": "Data Version",
        "unit_type": "Unit Type", "data_quality": "Data Quality",
    }
    active_tags = []
    for k, v in active.items():
        if k in SKIP_SUMMARY:
            continue
        if v and v not in ([], ""):
            label = LABEL_MAP.get(k, k.replace("_", " ").title())
            active_tags.append(f"**{label}:** {', '.join(str(x) for x in v) if isinstance(v, list) else v}")
    if active.get("active_only"):
        active_tags.append("**Status:** Active only")
    if active.get("license", "All") != "All":
        active_tags.append(f"**License:** {active['license']}")
    if active_tags:
        st.info("🔎 " + " | ".join(active_tags))

    # ── Reset page on filter change ──
    filter_key = str(sorted(str(active.items())))
    if st.session_state.get("_last_filter_key") != filter_key:
        st.session_state["page_num"] = 0
        st.session_state["_last_filter_key"] = filter_key

    # ── Fetch & render ──
    page = st.session_state.get("page_num", 0)

    with st.spinner("Querying…"):
        try:
            page_df, total, col_map = fetch_page(db_url, ef_table, src_table, active, page)
        except Exception as e:
            st.error("Query error. Please contact support.")
            st.exception(e)
            return

    if total == 0:
        st.warning("No results match your filters.")
        return

    render_pagination(total)
    render_table_header()
    for i, (_, row) in enumerate(page_df.iterrows()):
        render_row(row, i)
    render_pagination(total)


if __name__ == "__main__":
    main()