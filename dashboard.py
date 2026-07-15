import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

st.set_page_config(page_title="E-Commerce Real-Time Analytics", layout="wide")

# --- REAL-TIME CONFIG ---
# This refreshes the dashboard every 60 seconds
st_autorefresh(interval=60000, key="datarefresh")

st.title("📊 Data Engineering Pipeline: Live Insights")

# Connection
engine = create_engine(DB_URL)


def get_data():
    return pd.read_sql("SELECT * FROM sales_historical", engine)


data = get_data()

# --- TOP ROW: KPI METRICS ---
total_rev = data['amount'].sum()
col1, col2, col3 = st.columns(3)
col1.metric("Live Total Revenue", f"${total_rev:,.2f}")
col2.metric("Transaction Volume", f"{len(data):,}")
col3.metric("Latest Category", data.iloc[-1]['category'] if not data.empty else "N/A")

# --- MIDDLE ROW: VISUALIZATIONS ---
c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("1. Revenue Split (Batch)")
    fig1 = px.pie(data, values='amount', names='category', hole=0.4)
    st.plotly_chart(fig1, use_container_width=True)

with c2:
    st.subheader("2. Revenue Trend (Stream)")
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    trend = data.set_index('timestamp').resample('1min')['amount'].sum().reset_index()
    fig2 = px.line(trend, x='timestamp', y='amount', title="Sales per Minute")
    st.plotly_chart(fig2, use_container_width=True)

with c3:
    st.subheader("3. Category Performance")
    cat_perf = data.groupby('category')['amount'].sum().sort_values(ascending=False).reset_index()
    fig3 = px.bar(cat_perf, x='category', y='amount', color='category')
    st.plotly_chart(fig3, use_container_width=True)

st.subheader("Raw Stream Log (Latest 10)")
st.table(data.sort_values(by='timestamp', ascending=False).head(10))