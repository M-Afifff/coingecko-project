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
    """Load data from PostgreSQL using dbt marts"""
    
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
            WHERE latest_rank <= 20
        )
        ORDER BY extracted_date DESC, avg_market_cap_billions DESC
        """
        daily_df = pd.read_sql(daily_query, engine)
        
        # Load crypto performance analytics 
        performance_query = """
        SELECT 
            symbol, name, category, price_range, performance_name, signal_type,
            avg_price, avg_market_cap_billions, avg_price_change_24h, 
            best_rank, liquidity_status, date_actual
        FROM crypto_performance 
        WHERE date_actual >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date_actual DESC, best_rank ASC
        LIMIT 200
        """
        performance_df = pd.read_sql(performance_query, engine)
        
        # Load dimension data for filtering
        dims_query = """
        SELECT 
            'market_tier' as dim_type, tier_name as name, tier_description as description 
        FROM dim_market_tier
        UNION ALL
        SELECT 
            'price_category' as dim_type, category_name as name, category_description as description 
        FROM dim_price_category
        UNION ALL
        SELECT 
            'performance' as dim_type, performance_name as name, performance_description as description 
        FROM dim_performance
        """
        dims_df = pd.read_sql(dims_query, engine)
        
        return summary_df, daily_df, performance_df, dims_df
        
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.error(f"Using DATABASE_URL: {DATABASE_URL}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def main():
    # Header
    st.title("Cryptocurrency Analytics Dashboard")
    st.markdown("*CoinGecko ETL Pipeline with dbt Dimensional Modeling*")
    st.markdown("**Data Flow**: CoinGecko API → Python ETL → PostgreSQL → dbt (Star Schema) → Streamlit")
    
    # Load data
    with st.spinner("Loading data from dbt marts..."):
        summary_df, daily_df, performance_df, dims_df = load_data()
    
    if summary_df.empty:
        st.warning("No data available. Please run the ETL pipeline!")
        st.code("python -m etl.prefect_flow", language="bash")
        return
    
    # Sidebar with dimensional filters
    st.sidebar.header("Filters")
    
    # Basic filters
    top_n = st.sidebar.slider("Show Top N Cryptocurrencies", 10, 50, 20)
    
    # Dimensional filters
    if not summary_df.empty:
        market_tiers = ["All"] + sorted(summary_df['market_tier'].unique().tolist())
        selected_tier = st.sidebar.selectbox("Market Tier", market_tiers)
        
        performance_types = ["All"] + sorted(summary_df['performance_24h'].unique().tolist())
        selected_performance = st.sidebar.selectbox("Performance Type", performance_types)
        
        # Price range filter
        if not summary_df['latest_price'].empty:
            price_range = st.sidebar.slider(
                "Price Range ($)", 
                float(summary_df['latest_price'].min()), 
                float(summary_df['latest_price'].max()),
                (float(summary_df['latest_price'].min()), float(summary_df['latest_price'].max()))
            )
    
    # Filter data
    display_df = summary_df.head(top_n).copy()
    
    if not summary_df.empty:
        if selected_tier != "All":
            display_df = display_df[display_df['market_tier'] == selected_tier]
        if selected_performance != "All":
            display_df = display_df[display_df['performance_24h'] == selected_performance]
        display_df = display_df[
            (display_df['latest_price'] >= price_range[0]) & 
            (display_df['latest_price'] <= price_range[1])
        ]
    
    # Key Metrics
    st.subheader("Market Overview")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    if not display_df.empty:
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
            gainers = len(display_df[display_df['latest_price_change_24h'] > 0])
            st.metric("Gainers", f"{gainers}/{len(display_df)}")
        
        with col5:
            last_update = summary_df['last_updated'].max()
            st.metric("Last Updated", last_update.strftime("%H:%M"))
    
    # Charts Section
    st.subheader("Analytics Dashboard")
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["Market Overview", "Performance Analysis", "Category Breakdown", "Trends"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            if not display_df.empty:
                # Market cap chart
                st.subheader("Market Capitalization")
                fig_market_cap = px.bar(
                    display_df.head(15),
                    x='symbol',
                    y='latest_market_cap_billions',
                    hover_data=['name', 'latest_price', 'market_tier'],
                    title="Top 15 by Market Cap (Billions USD)",
                    color='market_tier',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig_market_cap.update_layout(height=400)
                st.plotly_chart(fig_market_cap, use_container_width=True)
        
        with col2:
            if not display_df.empty:
                # Price change chart with coloring
                st.subheader("24h Price Performance")
                fig_change = px.bar(
                    display_df.head(15),
                    x='symbol',
                    y='latest_price_change_24h',
                    color='performance_24h',
                    color_discrete_map={
                        'Strong Gain': '#00C851',
                        'Gain': '#4CAF50', 
                        'Loss': '#FF9800',
                        'Strong Loss': '#F44336'
                    },
                    title="24-Hour Price Changes (%)",
                    hover_data=['name', 'latest_price']
                )
                fig_change.update_layout(height=400)
                st.plotly_chart(fig_change, use_container_width=True)
    
    with tab2:
        if not performance_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Performance signal distribution
                st.subheader("Trading Signals Distribution")
                signal_counts = performance_df.groupby('signal_type').size().reset_index(name='count')
                fig_signals = px.pie(
                    signal_counts, 
                    values='count', 
                    names='signal_type',
                    title="Current Trading Signals",
                    color_discrete_map={'BUY': '#00C851', 'HOLD': '#4CAF50', 'SELL': '#F44336'}
                )
                st.plotly_chart(fig_signals, use_container_width=True)
            
            with col2:
                # Liquidity analysis
                st.subheader("Liquidity Status")
                liquidity_counts = performance_df.groupby('liquidity_status').size().reset_index(name='count')
                fig_liquidity = px.bar(
                    liquidity_counts,
                    x='liquidity_status',
                    y='count',
                    title="Cryptocurrency Liquidity Distribution",
                    color='liquidity_status'
                )
                st.plotly_chart(fig_liquidity, use_container_width=True)
    
    with tab3:
        if not performance_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Category analysis
                st.subheader("Category Performance")
                category_perf = performance_df.groupby('category').agg({
                    'avg_price_change_24h': 'mean',
                    'avg_market_cap_billions': 'sum'
                }).reset_index()
                
                fig_category = px.scatter(
                    category_perf,
                    x='avg_market_cap_billions',
                    y='avg_price_change_24h',
                    size='avg_market_cap_billions',
                    color='category',
                    title="Category Performance vs Market Cap",
                    hover_data=['category']
                )
                st.plotly_chart(fig_category, use_container_width=True)
            
            with col2:
                # Price range distribution
                st.subheader("Price Range Analysis")
                price_range_counts = performance_df.groupby('price_range').size().reset_index(name='count')
                fig_price_range = px.bar(
                    price_range_counts,
                    x='price_range',
                    y='count',
                    title="Distribution by Price Range",
                    color='price_range'
                )
                st.plotly_chart(fig_price_range, use_container_width=True)
    
    with tab4:
        if not daily_df.empty:
            # Time series trend
            st.subheader("Price Trends (Last 7 Days)")
            
            # Select top cryptocurrencies for trend analysis
            top_cryptos = daily_df.groupby('symbol')['avg_market_cap_billions'].max().nlargest(8).index
            trend_data = daily_df[daily_df['symbol'].isin(top_cryptos)]
            
            fig_trends = px.line(
                trend_data,
                x='extracted_date',
                y='avg_price',
                color='symbol',
                title="Price Trends - Top 8 Cryptocurrencies",
                hover_data=['name', 'avg_market_cap_billions']
            )
            fig_trends.update_layout(height=500)
            st.plotly_chart(fig_trends, use_container_width=True)
    
    # Data table
    st.subheader("Cryptocurrency Rankings")
    
    if not display_df.empty:
        # Format display data with columns
        display_cols = [
            'latest_rank', 'name', 'symbol', 'latest_price', 
            'latest_market_cap_billions', 'latest_price_change_24h', 
            'performance_24h', 'market_tier', 'volatility_tier'
        ]
        
        formatted_df = display_df[display_cols].copy()
        formatted_df['latest_price'] = formatted_df['latest_price'].apply(lambda x: f"${x:,.4f}")
        formatted_df['latest_market_cap_billions'] = formatted_df['latest_market_cap_billions'].apply(lambda x: f"${x:.2f}B")
        formatted_df['latest_price_change_24h'] = formatted_df['latest_price_change_24h'].apply(lambda x: f"{x:+.2f}%")
        
        # Rename columns for display
        formatted_df.columns = ['Rank', 'Name', 'Symbol', 'Price', 'Market Cap', '24h Change', 'Performance', 'Market Tier', 'Volatility']
        
        st.dataframe(formatted_df, use_container_width=True)
    
    # Technical info
    with st.expander("Pipeline Architecture"):
        st.markdown("""
        **Data Pipeline with Dimensional Modeling**:
        
        **1. Data Ingestion**
        - **Extract**: CoinGecko API → Python (50+ cryptocurrencies)
        - **Transform**: Data cleaning, validation, calculated fields
        - **Load**: PostgreSQL with proper indexing and constraints
        
        **2. Orchestration Layer**
        - **Prefect**: Task dependencies, retries, monitoring, logging
        - **Sequential execution**: ETL → dbt transformations
        
        **3. dbt Dimensional Modeling (Star Schema)**
        - **Staging**: `stg_crypto_prices` (clean, typed data)
        - **Intermediate**: `int_crypto_metrics` (business calculations)
        - **Dimension Tables**:
          - `dim_crypto` (cryptocurrency master data)
          - `dim_date` (date dimension 2020-2030)
          - `dim_market_tier` (ranking classifications)
          - `dim_price_category` (price range classifications)
          - `dim_performance` (performance signal classifications)
        - **Fact Tables**:
          - `crypto_daily` (daily aggregated metrics)
          - `crypto_summary` (latest snapshot)
        - **Analytics Views**: `crypto_performance` (business intelligence)
        
        **4. Visualization Layer**
        - **Streamlit**: Interactive dashboard with dimensional filtering
        - **Real-time refresh**: 5-minute cache with manual refresh option
        
        **Tech Stack**: Python, PostgreSQL, dbt, Prefect, Streamlit, Plotly
        """)
    
    # Data refresh section
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Management**")
    
    if not summary_df.empty:
        last_run = summary_df['last_updated'].max()
        st.sidebar.success(f"Updated: {last_run.strftime('%Y-%m-%d %H:%M')}")
        
        # Show data quality metrics
        total_records = len(summary_df)
        active_cryptos = len(summary_df[summary_df['data_quality_flag'] == 'Single Record'])
        st.sidebar.info(f"{total_records} total records")
    
    st.sidebar.markdown("**Pipeline Control**")
    if st.sidebar.button("Run Pipeline"):
        st.sidebar.info("Execute: `python -m etl.prefect_flow`")
        
if __name__ == "__main__":
    main()