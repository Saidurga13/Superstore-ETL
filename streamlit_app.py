import streamlit as st
import pandas as pd
import sqlite3
from etl.files.etl_data import extract, transform, load

st.title("ETL Automation with Streamlit")

file_path = './etl/files/superstore_data.csv'
db_name = 'etl_database.db'
table_name = 'transformed_table'

# Step 1: Extract
st.header("Step 1: Extract Data")
if st.button("Extract Data"):
    data = extract(file_path)
    if data is not None:
        st.write("Extracted Data Preview:")
        st.dataframe(data.head())
    else:
        st.error("Failed to extract data.")

# Step 2: Transform
if st.button("Transform Data"):
    data = extract(file_path)
    transformed_data = transform(data)
    if transformed_data is not None:
        st.write("Transformed Data Preview:")
        st.dataframe(transformed_data.head())
    else:
        st.error("Failed to transform data.")

# Step 3: Load
if st.button("Load Data"):
    data = extract(file_path)
    transformed_data = transform(data)
    load(transformed_data, db_name, table_name)
    st.success(f"Data loaded into database table: {table_name}")

# Display data from the database
if st.button("View Data in Database"):
    conn = sqlite3.connect(db_name)
    query = f"SELECT * FROM {table_name}"
    db_data = pd.read_sql(query, conn)
    conn.close()
    st.write("Data from Database:")
    st.dataframe(db_data)
