"""
מבצע מגש פיצה 🍕 - Streamlit Dashboard (Part 3)
דשבורד ניהולי: סטטוסים, אלרגנים, הזמנות אחרונות
"""

import os
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px

# ── Configuration ────────────────────────────────────────────
st.set_page_config(
    page_title="🍕 Pizza Ops Dashboard",
    page_icon="🍕",
    layout="wide"
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# ── MongoDB connection ───────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI)

mongo_client = get_mongo_client()
orders_col = mongo_client["pizza_ops"]["orders"]


# ── Data loading ─────────────────────────────────────────────

@st.cache_data(ttl=5)  # Cache for 5 seconds
def load_data():
    """טוען כל ההזמנות מ-MongoDB."""
    cursor = orders_col.find(
        {},
        {
            "_id": 0,
            "order_id": 1,
            "pizza_type": 1,
            "status": 1,
            "allergens_matched": 1,
            "update_time": 1,
            "insertion_time": 1,
        }
    )
    df = pd.DataFrame(list(cursor))
    return df


def get_status_counts(df):
    """ספירה לפי סטטוס."""
    if df.empty:
        return pd.Series(dtype=int)
    return df["status"].value_counts()


def get_top_allergens(df, top_n=10):
    """Top N allergens מתוך הזמנות שבוטלו."""
    cancelled = df[df["status"] == "CANCELLED"]
    
    if cancelled.empty or "allergens_matched" not in cancelled.columns:
        return pd.Series(dtype=int)
    
    # Explode lists → flatten
    allergens = cancelled["allergens_matched"].dropna()
    
    # אם זה list של lists
    flat_allergens = []
    for item in allergens:
        if isinstance(item, list):
            flat_allergens.extend(item)
        elif isinstance(item, str):
            flat_allergens.append(item)
    
    if not flat_allergens:
        return pd.Series(dtype=int)
    
    allergen_series = pd.Series(flat_allergens)
    return allergen_series.value_counts().head(top_n)


def get_last_10_orders(df):
    """10 הזמנות אחרונות לפי update_time."""
    if df.empty or "update_time" not in df.columns:
        return pd.DataFrame()
    
    # סנן הזמנות עם update_time
    with_time = df[df["update_time"].notna()].copy()
    
    if with_time.empty:
        return pd.DataFrame()
    
    # מיון יורד
    with_time = with_time.sort_values("update_time", ascending=False)
    
    # 10 ראשונות
    top10 = with_time.head(10)
    
    # עמודות לתצוגה
    display_cols = ["order_id", "pizza_type", "status", "allergens_matched", "update_time"]
    available_cols = [c for c in display_cols if c in top10.columns]
    
    return top10[available_cols]


# ── Main dashboard ───────────────────────────────────────────

def main():
    st.title("🍕 Pizza Operations Intelligence Dashboard")
    st.markdown("**Part 3** – Risk Analysis & Operational Monitoring")
    
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    
    # Load data
    with st.spinner("Loading data from MongoDB..."):
        df = load_data()
    
    if df.empty:
        st.warning("⚠️ No orders found in database")
        return
    
    total_orders = len(df)
    
    # ── Section 1: Status Distribution ──────────────────────
    st.header("📊 Order Status Distribution")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        status_counts = get_status_counts(df)
        
        if not status_counts.empty:
            fig_pie = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Status Breakdown",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No status data available")
    
    with col2:
        st.metric("**Total Orders**", total_orders)
        
        st.write("**Counts by Status:**")
        if not status_counts.empty:
            for status, count in status_counts.items():
                st.write(f"- **{status}**: {count}")
    
    st.divider()
    
    # ── Section 2: Top Allergens ────────────────────────────
    st.header("🚨 Top 10 Allergens (Cancellations)")
    
    top_allergens = get_top_allergens(df, top_n=10)
    
    if not top_allergens.empty:
        fig_bar = px.bar(
            x=top_allergens.values,
            y=top_allergens.index,
            orientation="h",
            title="Allergens Causing Most Cancellations",
            labels={"x": "Number of Cancellations", "y": "Allergen"},
            color=top_allergens.values,
            color_continuous_scale="Reds"
        )
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No allergen data available (no CANCELLED orders)")
    
    st.divider()
    
    # ── Section 3: Last 10 Orders ───────────────────────────
    st.header("📋 Last 10 Processed Orders")
    st.caption("Sorted by `update_time` (most recent first)")
    
    last_10 = get_last_10_orders(df)
    
    if not last_10.empty:
        st.dataframe(
            last_10,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No orders with `update_time` yet")
    
    st.divider()
    
    # Footer
    st.caption("🔬 Data refreshed every 5 seconds (Streamlit cache)")
    st.caption("🍕 Pizza Operations Dashboard – Part 3")


if __name__ == "__main__":
    main()
