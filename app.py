import streamlit as st
import pandas as pd
import ijson
import re
import tempfile

st.set_page_config(layout="wide", page_title="Data Explorer")

# -----------------------------
# Flatten JSON
# -----------------------------
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

# -----------------------------
# Stream JSON safely
# -----------------------------
def stream_json(file_path):
    with open(file_path, "rb") as f:
        for obj in ijson.items(f, "item"):
            yield flatten_json(obj)

# -----------------------------
# Load Data (chunk safe)
# -----------------------------
def load_data(file_path, max_rows=20000):
    data = []

    for i, record in enumerate(stream_json(file_path)):
        data.append(record)
        if i >= max_rows:
            break

    df = pd.DataFrame(data)
    df = df.fillna("")

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df

# -----------------------------
# FILTER UI
# -----------------------------
def render_filters(df):
    st.sidebar.header("🔍 Filters")

    FACETS = ["fields.category", "fields.sector", "fields.region", "fields.year"]

    if "filters" not in st.session_state:
        st.session_state.filters = {c: [] for c in FACETS}

    for col in FACETS:
        if col not in df.columns:
            continue

        label = col.split(".")[-1].capitalize()
        values = sorted(df[col].dropna().unique())

        selected = st.sidebar.multiselect(
            label,
            values,
            default=st.session_state.filters[col],
            key=f"f_{col}"
        )

        st.session_state.filters[col] = selected

    c1, c2 = st.sidebar.columns(2)

    if c1.button("Apply"):
        return {k: v for k, v in st.session_state.filters.items() if v}

    if c2.button("Clear"):
        st.session_state.filters = {c: [] for c in FACETS}
        st.rerun()

    return None

# -----------------------------
# Apply Filters
# -----------------------------
def apply_filters(df, filters):
    for col, vals in filters.items():
        vals = [v.lower().strip() for v in vals]

        df = df[
            df[col].astype(str).str.lower().str.strip().isin(vals)
        ]

    return df

# -----------------------------
# ROW UI (FIXED + CLEAN)
# -----------------------------
def render_row(row):
    raw_name = row.get("fields.name", "—")
    name = re.sub(r"\[.*?\]", "", raw_name).strip()

    with st.expander(f" {name}"):

        # Header section
        st.markdown("## Activity Overview")

        st.markdown(
            f"** Emission Factor:** `{row.get('fields.factor', '—')}`"
        )

        st.markdown("---")

        # Metadata section (FIXED TITLES)
        st.markdown("## Metadata")

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.markdown("**Activity ID**")
            st.write(row.get("fields.activity_id", "—"))

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

        # Detailed section
        st.markdown("##  Detailed Information")

        st.markdown("**Dataset**")
        st.write(row.get("fields.source_dataset", "—"))

        st.markdown("**Description**")
        st.write(row.get("fields.description", "—"))

        st.markdown("**Status**")
        st.write("Current")

# -----------------------------
# MAIN APP (CHUNK SAFE)
# -----------------------------
def main():
    st.title("📊 Data Explorer")

    uploaded_file = st.file_uploader("Upload JSON file", type=["json"])

    if uploaded_file:

        #  SAFE chunk write (no RAM explosion)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            while True:
                chunk = uploaded_file.read(1024 * 1024 * 5)  # 5MB chunks
                if not chunk:
                    break
                tmp.write(chunk)

            temp_path = tmp.name

        # Load streaming
        df = load_data(temp_path)

        # Filters
        filters = render_filters(df)

        if filters:
            df = apply_filters(df, filters)

        st.markdown(f"### Results ({len(df)})")

        if len(df) == 0:
            st.warning("No matching results found.")

        for _, row in df.iterrows():
            render_row(row)

    else:
        st.info("Upload a JSON file to begin analysis")

if __name__ == "__main__":
    main()