import streamlit as st
import duckdb
import pandas as pd
import tempfile
import os
 #python -m streamlit run "c:/Users/franc/OneDrive - Università Commerciale Luigi Bocconi/Academic/2. ESS/3. Tesi/src/parquet_viewer_streamlit_duck_db.py"
st.set_page_config(layout="wide")
st.title("Parquet Viewer")

# --- File selection ---
uploaded_file = st.file_uploader("Sfoglia e seleziona un file Parquet", type=["parquet"])

if uploaded_file is None:
    st.info("Carica un file .parquet per iniziare")
    st.stop()

# --- Save temp file ---
tmp_dir = tempfile.gettempdir()
file_path = os.path.join(tmp_dir, uploaded_file.name)

with open(file_path, "wb") as f:
    f.write(uploaded_file.read())

# --- DuckDB connection ---
con = duckdb.connect()

# --- Schema ---
schema_df = con.execute(f"""
    DESCRIBE SELECT * FROM read_parquet('{file_path}')
""").df()

st.subheader("Schema")
st.dataframe(schema_df, use_container_width=True)

# --- Row count ---
row_count = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet('{file_path}')
""").fetchone()[0]

st.write(f"Numero righe: {row_count}")

# --- Column selection ---
columns = schema_df["column_name"].tolist()
selected_cols = st.multiselect(
    "Colonne da visualizzare",
    columns,
    default=columns[:min(10, len(columns))]
)

# --- Filters ---
st.subheader("Filtri")
filters = []

for col in selected_cols:
    col_type = schema_df[schema_df["column_name"] == col]["column_type"].values[0]

    if "INT" in col_type or "DOUBLE" in col_type or "FLOAT" in col_type:
        min_val, max_val = con.execute(f"SELECT MIN(\"{col}\"), MAX(\"{col}\") FROM read_parquet('{file_path}')").fetchone()
        if min_val is not None and max_val is not None:
            val = st.slider(f"{col}", float(min_val), float(max_val), (float(min_val), float(max_val)))
            filters.append(f'"{col}" BETWEEN {val[0]} AND {val[1]}')

    elif "VARCHAR" in col_type or "TEXT" in col_type:
        uniques = con.execute(f"SELECT DISTINCT \"{col}\" FROM read_parquet('{file_path}') LIMIT 100").df()[col].dropna().tolist()
        if len(uniques) > 0:
            selected_vals = st.multiselect(f"{col}", uniques)
            if selected_vals:
                vals = ",".join([f"'{v}'" for v in selected_vals])
                filters.append(f'"{col}" IN ({vals})')

# --- Limit ---
limit = st.number_input("Numero righe", min_value=10, max_value=10000, value=100)

# --- Query build ---
if selected_cols:
    cols_sql = ", ".join([f'"{c}"' for c in selected_cols])
    where_clause = " AND ".join(filters) if filters else ""

    query = f"SELECT {cols_sql} FROM read_parquet('{file_path}')"
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f" LIMIT {limit}"

    st.subheader("Dati")
    df = con.execute(query).df()
    st.dataframe(df, use_container_width=True)

    # --- Download ---
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Scarica CSV", csv, "data.csv", "text/csv")

# --- Footer ---
st.caption("Powered by Streamlit + DuckDB")
