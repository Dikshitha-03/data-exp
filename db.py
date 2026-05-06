"""
db.py — Database helpers, schema discovery, filter option loading, and query builder.
"""

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def fetch_grouped_page(db_url: str, ef_table: str, src_table: str,
                       filters: dict, page: int) -> tuple:
    """
    Returns one row per distinct climatiq_id (activity_id), with:
      - activity_id, name, description, sector, category_name, lca_activity
      - factor_count  : how many individual records exist
      - years         : comma-separated distinct years
      - regions       : comma-separated distinct regions
      - source_names  : comma-separated distinct source names
    """
    col_map = build_column_map(db_url, ef_table)
    cid_col  = col_map.get("climatiq_id")
    name_col = col_map.get("name")
    if not cid_col or not name_col:
        # fallback to flat page if no activity_id column
        return fetch_page(db_url, ef_table, src_table, filters, page)

    src_cols     = get_table_columns(db_url, src_table) if src_table else []
    src_name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols), None)
    src_id_col   = col_map.get("source_id")

    join_sql = ""
    src_select = ""
    if src_id_col and src_table and src_name_col:
        # Source name comes from a joined source table
        join_sql   = f'LEFT JOIN "{src_table}" s ON ef."{src_id_col}"=s.id'
        src_select = f', STRING_AGG(DISTINCT s."{src_name_col}", \', \') AS source_names'
    elif src_id_col:
        # Source column is directly on the EF table (holds the name itself)
        src_select = f', STRING_AGG(DISTINCT CAST(ef."{src_id_col}" AS TEXT), \', \') AS source_names'

    where_sql, params = build_where(filters, col_map, src_name_col)

    year_col   = col_map.get("year")
    region_col = col_map.get("region")
    desc_col   = col_map.get("description")
    sect_col   = col_map.get("sector")
    cat_col    = col_map.get("category_name")
    lca_col    = col_map.get("lca_activity")

    year_agg   = f'STRING_AGG(DISTINCT CAST(ef."{year_col}" AS TEXT), \', \' ORDER BY CAST(ef."{year_col}" AS TEXT) DESC)' if year_col else "'—'"
    region_agg = f'STRING_AGG(DISTINCT ef."{region_col}", \', \')' if region_col else "'—'"
    desc_sel   = f'MIN(ef."{desc_col}")' if desc_col else "'—'"
    sect_sel   = f'MIN(ef."{sect_col}")' if sect_col else "'—'"
    cat_sel    = f'MIN(ef."{cat_col}")' if cat_col else "'—'"
    lca_sel    = f'MIN(ef."{lca_col}")' if lca_col else "'—'"

    count_sql = f"""
        SELECT COUNT(DISTINCT ef."{cid_col}") AS cnt
        FROM "{ef_table}" ef {join_sql} {where_sql}
    """
    count_df = run_query(db_url, count_sql, params or None)
    total = int(count_df["cnt"].iloc[0]) if not count_df.empty else 0

    offset = page * PAGE_SIZE
    data_sql = f"""
        SELECT
            ef."{cid_col}"   AS activity_id,
            MIN(ef."{name_col}") AS name,
            {desc_sel}       AS description,
            {sect_sel}       AS sector,
            {cat_sel}        AS category_name,
            {lca_sel}        AS lca_activity,
            COUNT(ef.*)      AS factor_count,
            {year_agg}       AS years,
            {region_agg}     AS regions
            {src_select}
        FROM "{ef_table}" ef {join_sql}
        {where_sql}
        GROUP BY ef."{cid_col}"
        ORDER BY MIN(ef."{name_col}")
        LIMIT %s OFFSET %s
    """
    data_df = run_query(db_url, data_sql,
                        (params + [PAGE_SIZE, offset]) if params else [PAGE_SIZE, offset])
    if not data_df.empty:
        data_df = data_df.fillna("—")
    return data_df, total, col_map


def fetch_children(db_url: str, ef_table: str, src_table: str,
                   activity_id: str, col_map: dict) -> pd.DataFrame:
    """Fetch all individual records for a given activity_id."""
    cid_col = col_map.get("climatiq_id")
    if not cid_col:
        return pd.DataFrame()

    sel_parts = []
    for canonical, actual in col_map.items():
        if canonical == actual:
            sel_parts.append(f'ef."{actual}"')
        else:
            sel_parts.append(f'ef."{actual}" AS "{canonical}"')
    ef_col_list = ", ".join(sel_parts) if sel_parts else "ef.*"

    src_cols     = get_table_columns(db_url, src_table) if src_table else []
    src_name_col = next((c for c in ["name", "source_name", "title"] if c in src_cols), None)
    src_url_col  = next((c for c in ["url", "link", "website"] if c in src_cols), None)
    src_id_col   = col_map.get("source_id")

    join_sql = ""
    src_select = ""
    if src_id_col and src_table and src_name_col:
        join_sql   = f'LEFT JOIN "{src_table}" s ON ef."{src_id_col}"=s.id'
        src_select = f', s."{src_name_col}" AS source_name'
        if src_url_col:
            src_select += f', s."{src_url_col}" AS source_url'

    year_col   = col_map.get("year")
    region_col = col_map.get("region")
    order_sql  = ""
    if year_col and region_col:
        order_sql = f'ORDER BY ef."{year_col}" DESC, ef."{region_col}"'
    elif year_col:
        order_sql = f'ORDER BY ef."{year_col}" DESC'

    df = run_query(db_url,
        f'SELECT {ef_col_list}{src_select} FROM "{ef_table}" ef {join_sql} '
        f'WHERE ef."{cid_col}" = %s {order_sql}',
        (activity_id,))
    if not df.empty:
        df = df.fillna("—")
    return df


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