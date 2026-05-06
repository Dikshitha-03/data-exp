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


# ─────────────────────────────────────────
# Row renderer — expandable accordion
# ─────────────────────────────────────────
def render_row(row, idx):

    def g(key):
        v = row.get(key, "—")
        return v if str(v).strip() not in ("", "nan", "None", "—", "NaT") else "—"

    name        = g("name")
    scope       = g("scope") if g("scope") != "—" else g("sector")
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

    VALID_UNIT_HINTS = ["kg", "tonne", "ton ", "kwh", "mwh", "gj", "mj", "km",
                        "liter", "litre", "m3", "usd", "eur", "gbp", "head",
                        "night", "room", "passenger", "vehicle", "kg co2"]
    unit_str = str(unit).lower()
    unit_ok = unit != "—" and any(h in unit_str for h in VALID_UNIT_HINTS)
    co2e_display = f"{_fmt_factor(co2e)}{' ' + unit if unit_ok else ''}" if co2e != "—" else "—"

    scope_colors = {
        "1": ("#fff3e0", "#e65100"),
        "2": ("#e8f5e9", "#2e7d32"),
        "3": ("#e3f2fd", "#1565c0"),
    }
    badge_bg, badge_fg = scope_colors.get(str(scope), ("#f3e5f5", "#6a1b9a"))
    badge_label = f"Scope {scope}" if str(scope) in ("1", "2", "3") else str(scope)
    badge_display = badge_label if len(badge_label) <= 18 else badge_label[:16] + "…"

    exp_key = f"exp_{idx}"
    if exp_key not in st.session_state:
        st.session_state[exp_key] = False

    cols = st.columns([3, 1, 1.2, 1.2, 0.7, 1])

    with cols[0]:
        arrow = "▼" if st.session_state[exp_key] else "▶"
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
        f"padding:3px 7px;border-radius:10px;font-size:11px;font-weight:700;"
        f"display:inline-block;white-space:normal;word-break:break-word;"
        f"line-height:1.4;'>{badge_label}</span>",
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