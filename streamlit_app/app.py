# streamlit_app/app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import snowflake.sqlalchemy

st.set_page_config(page_title="Snowflake + Streamlit", layout="wide")

# ---------------------------------------------
# Direct Snowflake credentials
# ---------------------------------------------
USER = "RAMAKRISHNAM"
PASSWORD = "Ramakrishna118143"
ACCOUNT = "RPJXZUZ-WG57165"
DATABASE = "FRAUD_DETECTION_DB"
SCHEMA = "RAW"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

# ---------------------------------------------
# Build SQLAlchemy URL
# ---------------------------------------------
def get_sqlalchemy_url():
    query_params = [
        f"warehouse={WAREHOUSE}",
        f"role={ROLE}" if ROLE else None
    ]
    query = "&".join([q for q in query_params if q])
    return (
        f"snowflake://{USER}:{PASSWORD}@{ACCOUNT}/"
        f"{DATABASE}/{SCHEMA}?{query}"
    )

# ---------------------------------------------
# Create SQLAlchemy engine
# ---------------------------------------------
def get_engine():
    return create_engine(get_sqlalchemy_url())

# ---------------------------------------------
# List tables inside schema
# ---------------------------------------------
def list_tables():
    engine = get_engine()
    sql = text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema AND table_type = 'BASE TABLE' ORDER BY table_name")
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": SCHEMA}).fetchall()
    return [r[0] for r in rows]

# ---------------------------------------------
# Read table (FINAL FIX: Raw DBAPI connection)
# ---------------------------------------------
def read_table(table_name, limit=1000):
    engine = get_engine()
    sql = f'SELECT * FROM {SCHEMA}."{table_name}" LIMIT {limit}'
    
    # Use raw DBAPI connection to avoid cursor errors
    with engine.connect() as conn:
        raw_conn = conn.connection  # Get underlying Snowflake connection
        df = pd.read_sql(sql, raw_conn)
    return df

# ---------------------------------------------
# Streamlit UI
# ---------------------------------------------
def main():
    st.title("📊 Fraud Detection – Snowflake Data Viewer")

    # Test connection first
    st.sidebar.header("🔌 Connection Status")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT CURRENT_WAREHOUSE()")).fetchone()
            st.sidebar.success("✅ Connected to Snowflake")
            st.sidebar.caption(f"Database: {DATABASE} | Schema: {SCHEMA} | Warehouse: {result[0]}")
    except Exception as e:
        st.sidebar.error(f"❌ Connection Failed: {e}")
        st.sidebar.info("Check credentials, warehouse, and network access")
        st.stop()

    # Table list
    st.sidebar.header("📁 Tables")
    try:
        tables = list_tables()
        if not tables:
            st.sidebar.warning("No tables found in RAW schema")
            st.stop()
        selected = st.sidebar.selectbox("Choose table", ["-- none --"] + tables)
        st.sidebar.info(f"Found {len(tables)} tables")
    except Exception as e:
        st.sidebar.error(f"Error retrieving tables: {e}")
        st.stop()

    # Show table
    if selected and selected != "-- none --":
        st.subheader(f"Preview of `{selected}`")
        with st.spinner("Loading data..."):
            try:
                df = read_table(selected)
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "📥 Download CSV", 
                    df.to_csv(index=False),
                    file_name=f"{selected}.csv",
                    mime="text/csv"
                )
                st.caption(f"Showing {len(df)} rows")
            except Exception as e:
                st.error(f"Error loading table: {e}")
                st.info("Table might be empty or have access issues")

    st.sidebar.markdown("---")
    st.sidebar.info("✅ Ready for ML model integration")

if __name__ == "__main__":
    main()
