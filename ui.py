"""
ui.py — UI components: CSS injection, sidebar filters, pagination, table header, and row renderer.
"""

import streamlit as st


# ─────────────────────────────────────────
# CSS + JS injection
# ─────────────────────────────────────────
def inject_styles():
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

    /* ── Nested child expanders ── */
    div[data-testid="stExpander"] {
        border: 1px solid #e8ecf2 !important;
        border-radius: 8px !important;
        margin-bottom: 4px !important;
        background: #ffffff !important;
        overflow: hidden !important;
    }
    div[data-testid="stExpander"]:hover {
        border-color: #cbd5e1 !important;
        box-shadow: 0 1px 6px rgba(15,23,42,0.06) !important;
    }
    div[data-testid="stExpander"] summary {
        font-size: 13px !important;
        font-weight: 500 !important;
        color: #1e293b !important;
        padding: 8px 14px !important;
        background: #fafbfc !important;
    }
    div[data-testid="stExpander"] summary:hover {
        background: #f1f5f9 !important;
    }
    div[data-testid="stExpander"] > div[data-testid="stExpanderDetails"] {
        padding: 0 !important;
        border-top: 1px solid #f1f5f9 !important;
    }

    /* ── Kill Streamlit default vertical gaps ── */
    div[data-testid="stVerticalBlock"] { gap: 0 !important; }
    div[data-testid="stVerticalBlockBorderWrapper"] { padding: 0 !important; margin: 0 !important; }
    div[data-testid="stMarkdownContainer"] { margin-top: 0 !important; margin-bottom: 0 !important; }
    div[data-testid="stMarkdownContainer"] p { margin: 0 !important; padding: 0 !important; }
    div[data-testid="stHorizontalBlock"] { gap: 4px !important; margin-bottom: 0 !important; margin-top: 0 !important; }
    div[data-testid="column"] { padding: 0 2px !important; }
    div[data-testid="stButton"] { margin: 0 !important; padding: 0 !important; }
    div[data-testid="stButton"] button { margin: 0 !important; padding: 4px 10px !important; line-height: 1.4 !important; }

    /* ── Child record card ── */
    .child-card {
        background: #ffffff;
        border: 1px solid #e8ecf2;
        border-radius: 8px;
        margin: 6px 0 6px 0;
        overflow: hidden;
    }
    .child-kv-table {
        width: 100%; border-collapse: collapse; font-size: 12px;
    }
    .child-kv-table tr { border-bottom: 1px solid #f1f5f9; }
    .child-kv-table tr:last-child { border-bottom: none; }
    .child-kv-table td { padding: 6px 12px; vertical-align: middle; }
    .child-kv-table td.ck-key {
        font-size: 10px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.06em; color: #64748b;
        width: 150px; min-width: 150px; white-space: nowrap;
        background: #f8fafc; border-right: 1px solid #f1f5f9;
    }
    .child-kv-table td.ck-val {
        color: #1e293b; font-size: 12.5px; word-break: break-word;
    }
    .child-desc {
        font-size: 12px; color: #64748b; line-height: 1.65;
        background: #f8fafc; border-top: 1px solid #f1f5f9;
        padding: 8px 14px;
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
def render_pagination(total: int, page_size: int) -> int:
    total_pages = max(1, (total + page_size - 1) // page_size)
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
def _fmt_factor(val) -> str:
    try:
        f = float(val)
        if f == 0: return "0"
        if abs(f) < 0.0001: return f"{f:.3e}"
        if abs(f) < 1: return f"{f:.6f}".rstrip("0").rstrip(".")
        return f"{f:.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(val)


# ─────────────────────────────────────────
# Table header
# ─────────────────────────────────────────
def render_table_header():
    cols = st.columns([3.5, 0.7, 1.5, 1.8, 1.5])
    headers = ["ACTIVITY NAME", "FACTORS", "SOURCE", "YEAR", "REGIONS"]
    for col, h in zip(cols, headers):
        col.markdown(
            f"<div style='font-size:10px;font-weight:700;letter-spacing:0.1em;"
            f"color:#94a3b8;padding-bottom:6px;border-bottom:2px solid #e2e8f0;'>"
            f"{h}</div>",
            unsafe_allow_html=True
        )
    st.markdown("<div style='margin-bottom:4px;'></div>", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────
VALID_UNIT_HINTS = ["kg", "tonne", "ton ", "kwh", "mwh", "gj", "mj", "km",
                    "liter", "litre", "m3", "usd", "eur", "gbp", "head",
                    "night", "room", "passenger", "vehicle", "kg co2"]

SCOPE_COLORS = {
    "1": ("#fff3e0", "#e65100"),
    "2": ("#e8f5e9", "#2e7d32"),
    "3": ("#e3f2fd", "#1565c0"),
}

def _g(row, key):
    v = row.get(key, "—")
    return v if str(v).strip() not in ("", "nan", "None", "—", "NaT") else "—"

def _co2e_display(co2e, unit):
    unit_ok = unit != "—" and any(h in str(unit).lower() for h in VALID_UNIT_HINTS)
    return f"{_fmt_factor(co2e)}{(' ' + unit) if unit_ok else ''}" if co2e != "—" else "—"

def _scope_badge(scope):
    bg, fg = SCOPE_COLORS.get(str(scope), ("#f3e5f5", "#6a1b9a"))
    label = f"Scope {scope}" if str(scope) in ("1", "2", "3") else str(scope)
    return bg, fg, label

def _field_html(label, value):
    color = "#94a3b8" if value == "—" else "#1e293b"
    extra = "font-style:italic;" if value == "—" else "font-weight:600;"
    return (f"<div style='margin-bottom:8px;'>"
            f"<div style='font-size:10px;font-weight:700;letter-spacing:0.07em;"
            f"text-transform:uppercase;color:#94a3b8;margin-bottom:2px;'>{label}</div>"
            f"<div style='background:#f1f5f9;border:1px solid #e2e8f0;padding:4px 10px;"
            f"border-radius:6px;font-size:12.5px;color:{color};{extra}word-break:break-word;'>"
            f"{value}</div></div>")

def _section_hdr(text, color, border):
    return (f"<div style='font-size:11px;font-weight:700;color:{color};"
            f"text-transform:uppercase;letter-spacing:0.07em;"
            f"border-bottom:2px solid {border};padding-bottom:4px;margin-bottom:8px;'>{text}</div>")


# ─────────────────────────────────────────
# Level-2: individual child record row
# Each record is a nested expander (dropdown within dropdown)
# ─────────────────────────────────────────
def _render_child(child_row, child_key, parent_name: str = "", parent_activity_id: str = ""):
    g = lambda k: _g(child_row, k)

    # ── Gather all fields ──
    scope         = g("scope") if g("scope") != "—" else g("sector")
    year          = g("year")
    year_released = g("year_released")
    region        = g("region")
    region_name   = g("region_name")
    country_code  = g("country_code")
    source        = g("source_name")
    source_url    = g("source_url") if "source_url" in child_row else "—"
    source_ref    = g("source_reference")
    co2e          = g("co2e_factor")
    co2_f         = g("co2_factor")
    ch4_f         = g("ch4_factor")
    n2o_f         = g("n2o_factor")
    bio_f         = g("biogenic_co2_factor")
    unit          = g("factor_unit")
    unit_type     = g("unit_type")
    input_unit    = g("input_unit")
    desc          = g("description")
    lca           = g("lca_activity")
    activity_type = g("activity_type")
    data_quality  = g("data_quality")
    confidence    = g("confidence_score")
    tags          = g("tags")
    is_active     = g("is_active")
    is_custom     = g("is_custom")
    created_at    = g("created_at")
    updated_at    = g("updated_at")
    climatiq      = g("climatiq_id")
    ext_ref       = g("external_ref")
    row_id        = g("id")

    child_name     = g("name") if g("name") != "—" else parent_name
    child_id_label = climatiq if climatiq != "—" else parent_activity_id

    year_suffix = f" · {year}" if year != "—" else ""
    lca_suffix  = f" · {lca}"  if lca  != "—" else ""
    name_line   = f"{child_name}{year_suffix}{lca_suffix}" if child_name != "—" else child_id_label
    show_id     = child_id_label if (child_id_label and child_id_label != child_name) else ""

    co2e_disp = _co2e_display(co2e, unit)
    bg, fg, badge_label = _scope_badge(scope)

    # region display: prefer region_name, fallback to region or country_code
    region_disp = region_name if region_name != "—" else (region if region != "—" else country_code)
    if region_disp != "—" and country_code != "—" and country_code not in region_disp:
        region_disp = f"{region_disp} ({country_code})"

    # Expander label
    label_parts = [name_line]
    if co2e_disp != "—":
        label_parts.append(f"  |  {co2e_disp}")
    if region_disp != "—":
        label_parts.append(f"  ·  {region_disp}")
    expander_label = "".join(label_parts)

    def _kv(k, v):
        return f"<tr><td class='ck-key'>{k}</td><td class='ck-val'>{v}</td></tr>"

    def _plain(val):
        return f"<span style='color:#475569;'>{val}</span>"

    def _mono(val):
        return f"<span style='font-family:monospace;font-size:11.5px;color:#334155;'>{val}</span>"

    def _green(val):
        return (f"<span style='font-family:monospace;font-size:12.5px;font-weight:700;"
                f"color:#166534;background:#f0fdf4;padding:2px 8px;border-radius:4px;'>{val}</span>")

    def _badge(bg_, fg_, label_):
        return (f"<span style='background:{bg_};color:{fg_};padding:2px 10px;"
                f"border-radius:8px;font-size:11px;font-weight:700;"
                f"display:inline-block;line-height:1.5;'>{label_}</span>")

    def _section_row(label):
        return (f"<tr><td colspan='2' style='background:#f1f5f9;padding:6px 12px;"
                f"font-size:10px;font-weight:700;text-transform:uppercase;"
                f"letter-spacing:0.09em;color:#64748b;border-top:2px solid #e2e8f0;"
                f"border-bottom:1px solid #e8ecf2;'>{label}</td></tr>")

    with st.expander(expander_label, expanded=False):
        rows_html = ""

        # ── Description block at top if present ──
        desc_html = ""
        if desc != "—":
            desc_html = f"<div class='child-desc'>{desc}</div>"

        # ══ MAIN INFO ══
        rows_html += _section_row("Emission Factor Info")

        # Activity
        act_val = f"<span style='font-size:13px;font-weight:600;color:#1e293b;line-height:1.4;'>{name_line}</span>"
        if show_id:
            act_val += f"<br>{_mono(show_id)}"
        rows_html += _kv("Activity", act_val)

        # Scope
        if scope != "—":
            rows_html += _kv("Scope", _badge(bg, fg, badge_label))

        # CO2e Factor
        if co2e_disp != "—":
            rows_html += _kv("CO₂e Factor", _green(co2e_disp))

        # Individual gas factors
        gas_parts = []
        if co2_f != "—":  gas_parts.append(f"CO₂: {_fmt_factor(co2_f)}")
        if ch4_f != "—":  gas_parts.append(f"CH₄: {_fmt_factor(ch4_f)}")
        if n2o_f != "—":  gas_parts.append(f"N₂O: {_fmt_factor(n2o_f)}")
        if bio_f != "—":  gas_parts.append(f"Biogenic CO₂: {_fmt_factor(bio_f)}")
        if gas_parts:
            rows_html += _kv("Gas Breakdown",
                f"<span style='font-family:monospace;font-size:12px;color:#475569;'>"
                + " &nbsp;·&nbsp; ".join(gas_parts) + "</span>")

        # Source
        if source != "—":
            src_val = source
            if source_url != "—":
                src_val = f"<a href='{source_url}' target='_blank' class='kv-link'>{source}</a>"
            rows_html += _kv("Source", src_val)

        # Source reference / dataset
        if source_ref != "—":
            rows_html += _kv("Source Dataset", _plain(source_ref))

        # Year & Year Released
        if year != "—":
            rows_html += _kv("Year", _plain(year))
        if year_released != "—":
            rows_html += _kv("Year Released", _plain(year_released))

        # Region
        if region_disp != "—":
            rows_html += _kv("Region", _plain(region_disp))

        # Unit type & factor unit
        if unit_type != "—":
            rows_html += _kv("Unit Type(s)", _plain(unit_type))
        if input_unit != "—":
            rows_html += _kv("Input Unit", _mono(input_unit))

        # LCA Activity
        if lca != "—":
            rows_html += _kv("LCA Activity", _plain(lca))

        # Activity type
        if activity_type != "—":
            rows_html += _kv("Activity Type", _plain(activity_type))

        # ══ DETAILED METADATA ══
        meta_rows = ""
        if climatiq != "—":
            meta_rows += _kv("Activity ID", _mono(climatiq))
        if row_id != "—":
            meta_rows += _kv("ID", _mono(row_id))
        if ext_ref != "—":
            meta_rows += _kv("External Ref", _mono(ext_ref))
        if data_quality != "—":
            meta_rows += _kv("Data Quality", _plain(data_quality))
        if confidence != "—":
            try:
                pct = int(float(confidence) * 100)
                bar = (f"<span style='display:inline-flex;align-items:center;gap:7px;'>"
                       f"<span style='width:70px;height:5px;background:#e2e8f0;border-radius:3px;"
                       f"display:inline-block;overflow:hidden;'>"
                       f"<span style='width:{pct}%;height:5px;background:#3b82f6;"
                       f"border-radius:3px;display:block;'></span></span>"
                       f"<span style='font-size:12px;color:#475569;'>{pct}%</span></span>")
                meta_rows += _kv("Confidence", bar)
            except Exception:
                meta_rows += _kv("Confidence", _plain(confidence))
        if tags != "—":
            tag_list = str(tags).strip("[]").replace("'", "").split(",")
            tag_html = " ".join(
                f"<span style='background:#f1f5f9;border:1px solid #e2e8f0;border-radius:4px;"
                f"padding:1px 7px;font-size:11px;color:#475569;margin-right:3px;'>{t.strip()}</span>"
                for t in tag_list if t.strip()
            )
            if tag_html:
                meta_rows += _kv("Tags", tag_html)
        if is_active != "—":
            active_html = ("<span class='status-active'>● Active</span>"
                           if str(is_active).lower() in ("true", "1", "yes")
                           else "<span class='status-inactive'>● Inactive</span>")
            meta_rows += _kv("Status", active_html)
        if is_custom != "—":
            lic = ("Premium" if str(is_custom).lower() in ("true", "1", "yes") else "Core")
            meta_rows += _kv("License", _plain(lic))
        if updated_at != "—":
            meta_rows += _kv("Last Updated",
                f"<span class='ts-mono'>{str(updated_at)[:19]}</span>")
        elif created_at != "—":
            meta_rows += _kv("Created",
                f"<span class='ts-mono'>{str(created_at)[:19]}</span>")

        if meta_rows:
            rows_html += _section_row("Detailed Metadata")
            rows_html += meta_rows

        st.markdown(
            f"<div class='child-card'>"
            f"<table class='child-kv-table'>{rows_html}</table>"
            f"{desc_html}"
            f"</div>",
            unsafe_allow_html=True
        )


# ─────────────────────────────────────────
# Level-1: activity group row
# ─────────────────────────────────────────
def render_group_row(group_row, idx, fetch_children_fn):
    g = lambda k: _g(group_row, k)

    activity_id  = g("activity_id")
    name         = g("name")
    desc         = g("description")
    sector       = g("sector")
    category     = g("category_name")
    lca          = g("lca_activity")
    factor_count = g("factor_count")
    years        = g("years")
    regions      = g("regions")
    source_names = g("source_names")

    exp_key = f"grp_exp_{idx}"
    if exp_key not in st.session_state:
        st.session_state[exp_key] = False

    is_open = st.session_state[exp_key]

    with st.container(border=True):
        cols = st.columns([3.5, 0.7, 1.5, 1.8, 1.5])
        with cols[0]:
            arrow = "▼" if is_open else "▶"
            if st.button(f"{arrow}  {name}", key=f"grpbtn_{idx}", use_container_width=True):
                st.session_state[exp_key] = not st.session_state[exp_key]
                st.rerun()
        cols[1].markdown(
            f"<div style='padding-top:4px;'>"
            f"<span style='background:#2563eb;color:#fff;font-size:11px;font-weight:700;"
            f"padding:2px 9px;border-radius:20px;'>{factor_count}</span></div>",
            unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12.5px;color:#475569;'>{source_names}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:12.5px;color:#475569;'>{years}</span>", unsafe_allow_html=True)
        cols[4].markdown(f"<span style='font-size:12.5px;color:#475569;'>{regions}</span>", unsafe_allow_html=True)

        if is_open:
            st.markdown(
                "<hr style='margin:4px 0 8px 0;border:none;border-top:1px solid #e2e8f0;'>",
                unsafe_allow_html=True
            )

            # ── Chips row ──
            CHIP_STYLE = ("background:#f1f5f9;border:1px solid #e2e8f0;border-radius:4px;"
                          "padding:2px 10px;font-size:12px;color:#475569;margin-right:6px;"
                          "display:inline-block;margin-bottom:4px;")
            ID_STYLE   = ("background:#f3f0ff;border:1px solid #e9d5ff;border-radius:4px;"
                          "padding:2px 10px;font-size:12px;color:#6d28d9;font-family:monospace;"
                          "margin-right:6px;display:inline-block;margin-bottom:4px;")

            chips_html = ""
            if sector != "—":
                chips_html += f"<span style='{CHIP_STYLE}'>Sector: <b>{sector}</b></span>"
            if category != "—":
                chips_html += f"<span style='{CHIP_STYLE}'>Category: <b>{category}</b></span>"
            if lca != "—":
                chips_html += f"<span style='{CHIP_STYLE}'>LCA: <b>{lca}</b></span>"
            if activity_id != "—":
                chips_html += f"<span style='{ID_STYLE}'>{activity_id}</span>"

            if chips_html:
                st.markdown(
                    f"<div style='padding:2px 0 6px 0;'>{chips_html}</div>",
                    unsafe_allow_html=True
                )

            # ── Children ──
            children_key = f"children_{activity_id}"
            if children_key not in st.session_state:
                st.session_state[children_key] = fetch_children_fn(activity_id)
            children_df = st.session_state[children_key]

            if children_df.empty:
                st.markdown(
                    "<div style='color:#94a3b8;font-size:12px;padding:6px 4px;'>No records found.</div>",
                    unsafe_allow_html=True
                )
            else:
                for ci, (_, child) in enumerate(children_df.iterrows()):
                    _render_child(child, f"{idx}_{ci}", parent_name=name, parent_activity_id=activity_id)