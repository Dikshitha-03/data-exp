import streamlit as st
import pandas as pd
import ijson

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
# Stream JSON
# -----------------------------
def stream_json(file):
    objects = ijson.items(file, "item")
    for obj in objects:
        yield flatten_json(obj)

# -----------------------------
# Load Data
# -----------------------------
def load_data(file, max_rows=20000):
    data = []
    for i, record in enumerate(stream_json(file)):
        data.append(record)
        if i >= max_rows:
            break

    df = pd.DataFrame(data)

    # Clean data
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
        st.session_state.filters = {col: [] for col in FACETS}

    for col in FACETS:
        if col not in df.columns:
            continue

        label = col.split('.')[-1].capitalize()
        st.sidebar.markdown(f"### {label}")

        values = sorted(df[col].dropna().unique())

        selected = st.sidebar.multiselect(
            f"Select {label}",
            options=values,
            default=st.session_state.filters.get(col, []),
            key=f"ui_{col}"
        )

        st.session_state.filters[col] = selected

    col1, col2 = st.sidebar.columns(2)
    apply_clicked = col1.button("Apply")
    clear_clicked = col2.button("Clear")

    if clear_clicked:
        st.session_state.filters = {col: [] for col in FACETS}
        st.rerun()

    if apply_clicked:
        return {
            col: vals
            for col, vals in st.session_state.filters.items()
            if vals
        }

    return None

# -----------------------------
# Apply Filters
# -----------------------------
def apply_filters(df, filters):
    filtered = df.copy()

    for col, vals in filters.items():
        clean_vals = [str(v).strip().lower() for v in vals]

        filtered = filtered[
            filtered[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(clean_vals)
        ]

    return filtered

# -----------------------------
# Render Row UI (UPDATED)
# -----------------------------
def render_row(row):
    activity_id = row.get("fields.activity_id", "—")
    name = row.get("fields.name", "—")
    factor = row.get("fields.factor", "—")
    source = row.get("fields.source", "—")
    year = row.get("fields.year", "—")
    region = row.get("fields.region", "—")

    # ✅ ONLY activity name in collapsed view
    with st.expander(name):

        # ✅ Factor shown AFTER opening
        st.caption("Emission Factor")

        st.markdown(
            f"""
            <div style='background-color:#1A73E8;
                        color:white;
                        padding:8px 14px;
                        border-radius:16px;
                        display:inline-block;
                        font-weight:600;
                        margin-bottom:10px'>
                {factor}
            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown("---")

        # Summary row
        c1, c2, c3, c4 = st.columns(4)

        c1.markdown(f"**Activity ID**  \n{activity_id}")
        c2.markdown(f"**Source**  \n{source}")
        c3.markdown(f"**Year**  \n{year}")
        c4.markdown(f"**Region**  \n{region}")

        st.markdown("---")

        # Detailed info
        st.markdown("### Detailed Information")
        st.write(f"**Dataset:** {row.get('fields.source_dataset', '—')}")
        st.write(f"**Description:** {row.get('fields.description', '—')}")
        st.write("**Status:** Current")

# -----------------------------
# MAIN APP
# -----------------------------
def main():
    st.title("📊 Data Explorer")

    file = st.file_uploader("Upload JSON", type=["json"])

    if file:
        df = load_data(file)

        filters = render_filters(df)

        if filters is not None:
            df = apply_filters(df, filters)

        st.markdown(f"### Results ({len(df)})")

        if len(df) == 0:
            st.warning("No results found. Try different filters.")

        for _, row in df.iterrows():
            render_row(row)

    else:
        st.info("Upload a JSON file to begin")

# -----------------------------
if __name__ == "__main__":
    main()