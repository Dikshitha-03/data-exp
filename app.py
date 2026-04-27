import streamlit as st
import pandas as pd
import ijson
import re
import os

st.set_page_config(layout="wide", page_title="Data Explorer")

# -----------------------------
# SESSION STATE
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = None

if "filtered_df" not in st.session_state:
    st.session_state.filtered_df = None


# -----------------------------
# STREAM JSON (NO FULL LOAD)
# -----------------------------
def stream_json(file_path):
    with open(file_path, "rb") as f:
        for obj in ijson.items(f, "item"):
            yield obj


# -----------------------------
# FLATTEN JSON
# -----------------------------
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


# -----------------------------
# LOAD DATA (STREAMING)
# -----------------------------
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


# -----------------------------
# FILTER
# -----------------------------
def apply_filters(df, filters):
    for col, vals in filters.items():
        vals = [v.lower().strip() for v in vals]

        df = df[
            df[col].astype(str).str.lower().str.strip().isin(vals)
        ]

    return df


# -----------------------------
# UI ROW
# -----------------------------
def render_row(row):
    name = re.sub(r"\[.*?\]", "", str(row.get("fields.name", "—"))).strip()

    with st.expander(name):

        st.markdown("Details")

        st.write("Factor:", row.get("fields.factor", "—"))

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.write("Activity ID")
            st.write(row.get("fields.activity_id", "—"))

        with c2:
            st.write("Source")
            st.write(row.get("fields.source", "—"))

        with c3:
            st.write("Year")
            st.write(row.get("fields.year", "—"))

        with c4:
            st.write("Region")
            st.write(row.get("fields.region", "—"))

        st.markdown("Description")
        st.write(row.get("fields.description", "—"))


# -----------------------------
# FILTER UI
# -----------------------------
def render_filters(df):
    st.sidebar.header("Filters")

    FACETS = ["fields.category", "fields.sector", "fields.region", "fields.year"]

    if "filters" not in st.session_state:
        st.session_state.filters = {c: [] for c in FACETS}

    for col in FACETS:
        if col not in df.columns:
            continue

        values = sorted(df[col].dropna().unique())

        selected = st.sidebar.multiselect(
            col.split(".")[-1],
            values,
            default=st.session_state.filters[col],
            key=col
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
# MAIN APP
# -----------------------------
def main():
    st.title("Data Explorer")

    file_path = st.text_input("Enter full JSON file path")

    if file_path:

        if not os.path.exists(file_path):
            st.error("File not found. Check path.")
            return

        if st.session_state.df is None:
            with st.spinner("Loading data..."):
                st.session_state.df = load_data(file_path)

        df = st.session_state.df

        filters = render_filters(df)

        if filters:
            st.session_state.filtered_df = apply_filters(df, filters)
        else:
            st.session_state.filtered_df = df

        view_df = st.session_state.filtered_df

        st.write("Results:", len(view_df))

        MAX_ROWS = 200

        for _, row in view_df.head(MAX_ROWS).iterrows():
            render_row(row)

    else:
        st.info("Enter file path to start")


if __name__ == "__main__":
    main()