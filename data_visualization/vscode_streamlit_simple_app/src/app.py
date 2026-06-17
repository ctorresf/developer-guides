import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="KPI Regional Monitor", page_icon="📊", layout="wide")

st.title("📊 Consolidated Sales Monitor LATAM - FinOps Panel")
st.markdown("---")

sales_path = "data/gold_sales.csv"
dlq_path = "data/dlq_errors.json"

# Local infrastructure verification
if os.path.exists(sales_path) and os.path.exists(dlq_path):
    
    # Reading the pipeline data layers
    df_gold = pd.read_csv(sales_path)
    df_dlq = pd.read_json(dlq_path, lines=True)
    
    # Automated Observability Block (Quality Metrics)
    total_valid = len(df_gold)
    total_corrupts = len(df_dlq)
    total_universe = total_valid + total_corrupts
    error_rate = (total_corrupts / total_universe) * 100
    
    # Intelligent reactive interface against failures in the Data Contract
    if total_corrupts > 0:
        st.error(f"⚠️ **Data Quality Alert:** Isolated **{total_corrupts} records** in the Dead-Letter Queue ({error_rate:.1f}% of the daily batch of Buenos Aires).")
        with st.expander("View details of isolated corrupt records for audit"):
            st.dataframe(df_dlq, use_container_width=True)
    else:
        st.success("✅ Data contract validated at 100% for all regional offices.")
        
    # Deployment of Business KPIs
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="Total Sales Consolidated (USD)", value=f"${df_gold['monto_usd'].sum():,.2f}")
    with col2:
        st.metric(label="Successful Transactions Processed", value=f"{total_valid} items")
    with col3:
        st.metric(label="Regional Average Ticket (USD)", value=f"${df_gold['monto_usd'].mean():,.2f}")
        
    st.subheader("📋 Details of the Consolidated Operation (Gold Layer)")
    st.dataframe(df_gold, use_container_width=True)

else:
    st.warning("Waiting for the Apache Beam pipeline to initialize the data files...")