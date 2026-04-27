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
        objects = ijson.items(f, "item")
        for obj in objects:
            yield flatten_json(obj)

# -----------------------------
# Load data
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
# FILTERS
# -----------------------------
def render_filters(df):
    st.sidebar.header("🔍 Filters")

    FACETS = ["fields.category", "fields.sector", "fields.region", "fields.year"]

    if "filters" not in st.session_state:
        st.session_state.filters = {col: [] for col in FACETS}

    for col in FACETS:
        if col not in df.columns:
            continue

        label = col.split('.')[-1].capitalize()
        values = sorted(df[col].dropna().unique())

        selected = st.sidebar.multiselect(
            f"{label}",
            options=values,
            default=st.session_state.filters.get(col, []),
            key=f"ui_{col}"
        )

        st.session_state.filters[col] = selected

    col1, col2 = st.sidebar.columns(2)

    if col1.button("Apply"):
        return {k: v for k, v in st.session_state.filters.items() if v}

    if col2.button("Clear"):
        st.session_state.filters = {col: [] for col in FACETS}
        st.rerun()

    return None

# -----------------------------
# Apply filters
# -----------------------------
def apply_filters(df, filters):
    for col, vals in filters.items():
        vals = [v.lower().strip() for v in vals]

        df = df[
            df[col].astype(str).str.lower().str.strip().isin(vals)
        ]

    return df

# -----------------------------
# Row UI
# -----------------------------
def render_row(row):
    raw_name = row.get("fields.name", "—")
    name = re.sub(r"\[.*?\]", "", raw_name).strip()

    with st.expander(name):

        st.markdown(
            f"**Factor:** {row.get('fields.factor','—')}"
        )

        c1, c2, c3, c4 = st.columns(4)

        c1.write(f"ID: {row.get('fields.activity_id','—')}")
        c2.write(f"Source: {row.get('fields.source','—')}")
        c3.write(f"Year: {row.get('fields.year','—')}")
        c4.write(f"Region: {row.get('fields.region','—')}")

        st.markdown("---")
        st.write(row.get("fields.description", "—"))

# -----------------------------
# MAIN APP
# -----------------------------
def main():
    st.title("📊 Data Explorer")

    # ✅ Manual file selection (NOT upload memory)
    uploaded_file = st.file_uploader("Choose JSON file", type=["json"])

    if uploaded_file:

        # 🔥 Save temporarily to disk (critical fix)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            tmp.write(uploaded_file.read())
            temp_path = tmp.name

        # Load from disk stream (safe for huge files)
        df = load_data(temp_path)

        filters = render_filters(df)

        if filters:
            df = apply_filters(df, filters)

        st.markdown(f"### Results ({len(df)})")

        for _, row in df.iterrows():
            render_row(row)

    else:
        st.info("Select a JSON file to begin")

if __name__ == "__main__":
    main()