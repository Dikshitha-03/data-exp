import streamlit as st
import pandas as pd
import ijson
import tempfile
import os
import re

st.set_page_config(layout="wide", page_title="Data Explorer")

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
for key, default in {
    "tmp_path": None,
    "upload_name": None,
    "df": None,
    "filtered_df": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ─────────────────────────────────────────────
# SAFE CHUNKED UPLOAD (FIXED)
# ─────────────────────────────────────────────
def save_upload_chunked(uploaded_file):
    """
    Stream upload safely to disk.
    FIX: Streamlit does NOT support .chunks()
    """
    suffix = ".json"

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)

    uploaded_file.seek(0)

    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB safe chunk

    while True:
        chunk = uploaded_file.read(CHUNK_SIZE)
        if not chunk:
            break
        tmp.write(chunk)

    tmp.close()
    return tmp.name


# ─────────────────────────────────────────────
# STREAM JSON (NO FULL LOAD)
# ─────────────────────────────────────────────
def stream_json(file_path):
    with open(file_path, "rb") as f:
        for obj in ijson.items(f, "item"):
            yield obj


# ─────────────────────────────────────────────
# FLATTEN JSON
# ─────────────────────────────────────────────
def flatten_json(y, parent_key="", sep="."):
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


# ─────────────────────────────────────────────
# LOAD DATA (CHUNK PIPELINE STYLE)
# ─────────────────────────────────────────────
def load_data(file_path, max_rows=20000):
    data = []

    for i, record in enumerate(stream_json(file_path)):
        data.append(flatten_json(record))

        if i >= max_rows:
            break

    df = pd.DataFrame(data).fillna("")

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df


# ─────────────────────────────────────────────
# FILTER ENGINE
# ─────────────────────────────────────────────
def apply_filters(df, filters):
    for col, vals in filters.items():
        vals = [v.lower().strip() for v in vals]

        df = df[
            df[col].astype(str).str.lower().str.strip().isin(vals)
        ]

    return df


# ─────────────────────────────────────────────
# ROW UI (FIXED TITLES)
# ─────────────────────────────────────────────
def render_row(row):
    name = re.sub(r"\[.*?\]", "", row.get("fields.name", row.get("name", "—"))).strip()

    with st.expander(f" {name}"):

        st.markdown("###  Details")

        st.markdown(f"**Factor:** `{row.get('fields.factor', '—')}`")

        st.markdown("---")

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.markdown("**Activity ID**")
            st.write(row.get("fields.activity_id", row.get("activity_id", "—")))

        with c2:
            st.markdown("**Source**")
            st.write(row.get("fields.source", "—"))

        with c3:
            st.markdown("**Year**")
            st.write(row.get("fields.year", "—"))

        with c4:
            st.markdown("**Region**")
            st.write(row.get("fields.region", "—"))

        st.markdown("---")

        st.markdown("###  Description")
        st.write(row.get("fields.description", "—"))


# ─────────────────────────────────────────────
# FILTER UI
# ─────────────────────────────────────────────
def render_filters(df):
    st.sidebar.header("🔍 Filters")

    FACETS = ["fields.category", "fields.sector", "fields.region", "fields.year"]

    if "filters" not in st.session_state:
        st.session_state.filters = {c: [] for c in FACETS}

    for col in FACETS:
        if col not in df.columns:
            continue

        values = sorted(df[col].dropna().unique())

        selected = st.sidebar.multiselect(
            col.split(".")[-1].capitalize(),
            values,
            default=st.session_state.filters[col],
            key=f"f_{col}",
        )

        st.session_state.filters[col] = selected

    c1, c2 = st.sidebar.columns(2)

    if c1.button("Apply"):
        return {k: v for k, v in st.session_state.filters.items() if v}

    if c2.button("Clear"):
        st.session_state.filters = {c: [] for c in FACETS}
        st.rerun()

    return None


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def main():
    st.title("📊 Data Explorer")

    uploaded_file = st.file_uploader(
        "Upload JSON file",
        type=["json", "jsonl", "gz"],
    )

    # ── SAVE FILE SAFELY
    if uploaded_file:

        if uploaded_file.name != st.session_state.upload_name:

            if st.session_state.tmp_path and os.path.exists(st.session_state.tmp_path):
                try:
                    os.unlink(st.session_state.tmp_path)
                except:
                    pass

            with st.spinner("Saving file safely (chunked stream)…"):
                st.session_state.tmp_path = save_upload_chunked(uploaded_file)
                st.session_state.upload_name = uploaded_file.name

            st.success("File loaded successfully")

    # ── LOAD DATA ONLY ONCE
    if st.session_state.tmp_path and st.session_state.df is None:

        with st.spinner("Streaming JSON (ijson)…"):
            st.session_state.df = load_data(st.session_state.tmp_path)

    df = st.session_state.df

    if df is None:
        st.info("Upload a JSON file to begin")
        return

    # ── FILTERS
    filters = render_filters(df)

    if filters:
        st.session_state.filtered_df = apply_filters(df, filters)
    else:
        st.session_state.filtered_df = df

    view_df = st.session_state.filtered_df

    # ── OUTPUT
    st.markdown(f"### Results: {len(view_df)} rows")

    if len(view_df) == 0:
        st.warning("No results found")

    MAX_ROWS = 200

    for _, row in view_df.head(MAX_ROWS).iterrows():
        render_row(row)

    if len(view_df) > MAX_ROWS:
        st.info(f"Showing first {MAX_ROWS} rows for performance")


if __name__ == "__main__":
    main()