import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add parent directory to path to import ETL config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import DATABASE_URL
from sqlalchemy import create_engine

# Page configuration
st.set_page_config(
    page_title="Cryptocurrency Analytics - CoinGecko API",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=300)
def load_data():
    """Load data from PostgreSQL"""
    
    try:
        engine = create_engine(DATABASE_URL)
        
        # Load latest summary data (from dbt marts)
        summary_query = "SELECT * FROM crypto_summary ORDER BY latest_rank LIMIT 50"
        summary_df = pd.read_sql(summary_query, engine)
        
        # Load daily trends for charts (from dbt marts)
        daily_query = """
        SELECT * FROM crypto_daily 
        WHERE extracted_date >= CURRENT_DATE - INTERVAL '7 days'
        AND crypto_id IN (
            SELECT crypto_id FROM crypto_summary 
            WHERE latest_rank <= 10
        )
        ORDER BY extracted_date DESC, avg_market_cap_billions DESC
        """
        daily_df = pd.read_sql(daily_query, engine)
        
        return summary_df, daily_df
        
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.error(f"Using DATABASE_URL: {DATABASE_URL}")
        return pd.DataFrame(), pd.DataFrame()

def main():
    # Header
    st.title("Cryptocurrency Analytics")
    st.markdown("*CoinGecko Project - ETL Pipeline*")
    st.markdown("**Data Flow**: CoinGecko API -> Python ETL -> PostgreSQL -> dbt (staging -> intermediate -> marts) -> Streamlit")
    
    # Load data
    with st.spinner("Loading data from dbt marts..."):
        summary_df, daily_df = load_data()
    
    if summary_df.empty:
        st.warning("No data available. Please run ETL pipeline!")
        st.code("python -m etl.prefect_flow", language="bash")
        return
    
    # Sidebar
    st.sidebar.header("Filters")
    top_n = st.sidebar.slider("Show Top N Cryptocurrencies", 10, 50, 20)
    market_tier = st.sidebar.selectbox(
        "Market Tier", 
        ["All"] + list(summary_df['market_tier'].unique())
    )
    
    # Filter data
    display_df = summary_df.head(top_n)
    if market_tier != "All":
        display_df = display_df[display_df['market_tier'] == market_tier]
    
    # Key Metrics
    st.subheader("Market Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_cryptos = len(display_df)
        st.metric("Cryptocurrencies", total_cryptos)
    
    with col2:
        total_market_cap = display_df['latest_market_cap_billions'].sum()
        st.metric("Total Market Cap", f"${total_market_cap:.1f}B")
    
    with col3:
        avg_change = display_df['latest_price_change_24h'].mean()
        st.metric("Avg 24h Change", f"{avg_change:.2f}%")
    
    with col4:
        last_update = summary_df['last_updated'].max()
        st.metric("Last Updated", last_update.strftime("%H:%M"))
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Market cap chart
        st.subheader("Market Capitalization")
        fig_market_cap = px.bar(
            display_df.head(15),
            x='symbol',
            y='latest_market_cap_billions',
            hover_data=['name', 'latest_price'],
            title="Top 15 by Market Cap (Billions USD)",
            color='latest_market_cap_billions',
            color_continuous_scale='viridis'
        )
        fig_market_cap.update_layout(height=400)
        st.plotly_chart(fig_market_cap, use_container_width=True)
    
    with col2:
        # Price change chart
        st.subheader("24h Price Performance")
        fig_change = px.bar(
            display_df.head(15),
            x='symbol',
            y='latest_price_change_24h',
            color='latest_price_change_24h',
            color_continuous_scale='RdYlGn',
            title="24-Hour Price Changes (%)"
        )
        fig_change.update_layout(height=400)
        st.plotly_chart(fig_change, use_container_width=True)
    
    # Data table
    st.subheader("Cryptocurrency Rankings")
    
    # Format display data
    display_cols = [
        'latest_rank', 'name', 'symbol', 'latest_price', 
        'latest_market_cap_billions', 'latest_price_change_24h', 
        'performance_24h', 'market_tier'
    ]
    
    formatted_df = display_df[display_cols].copy()
    formatted_df['latest_price'] = formatted_df['latest_price'].apply(lambda x: f"${x:,.4f}")
    formatted_df['latest_market_cap_billions'] = formatted_df['latest_market_cap_billions'].apply(lambda x: f"${x:.2f}B")
    formatted_df['latest_price_change_24h'] = formatted_df['latest_price_change_24h'].apply(lambda x: f"{x:+.2f}%")
    
    # Rename columns for display
    formatted_df.columns = ['Rank', 'Name', 'Symbol', 'Price', 'Market Cap', '24h Change', 'Performance', 'Tier']
    
    st.dataframe(formatted_df, use_container_width=True)
    
    # Technical info
    with st.expander("Pipeline Architecture"):
        st.markdown("""
        **Complete Data Pipeline with Prefect Orchestration**:
        1. **Extract**: CoinGecko API -> Python (50 cryptocurrencies)
        2. **Transform**: Data transformation, cleaning, calculated fields
        3. **Load**: PostgreSQL with indexing and constraints
        4. **Orchestration**: Prefect with automatic retries and monitoring
        5. **dbt Transformations**:
           - `staging/stg_crypto_prices`
           - `intermediate/int_crypto_metrics`
           - `marts/crypto_daily`
           - `marts/crypto_summary`
        6. **Dashboard**: Streamlit visualization
        
        **Tech Stack**: Python, PostgreSQL, dbt, Prefect, Streamlit, Docker
        
        **Orchestration**: Prefect with task retries, dependency management, and logging/monitoring
        """)
    
    # Data refresh
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Refresh**")
    if not summary_df.empty:
        last_run = summary_df['last_updated'].max()
        st.sidebar.success(f"Updated: {last_run.strftime('%Y-%m-%d %H:%M')}")
    
    st.sidebar.markdown("**Refresh Data**")
    if st.sidebar.button("Run Prefect Pipeline"):
        st.sidebar.info("Run: `python -m etl.prefect_flow` in terminal")

if __name__ == "__main__":
    main()