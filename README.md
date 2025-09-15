# Cryptocurrency ETL Pipeline

An end-to-end data engineering pipeline that extracts cryptocurrency data from the CoinGecko API, transforms it using dimensional modeling techniques, and loads it into a PostgreSQL data warehouse with automated orchestration.

## Architecture Overview

```
CoinGecko API → Python ETL → PostgreSQL → dbt → Streamlit Dashboard
```

**Components:**
- **Extract**: CoinGecko API integration for real-time cryptocurrency data
- **Transform**: Python data processing with validation and calculations
- **Load**: PostgreSQL database with proper indexing and constraints
- **Orchestration**: Prefect workflow management with retry logic and monitoring
- **Analytics**: dbt dimensional modeling with staging, intermediate, and mart layers
- **Visualization**: Interactive Streamlit dashboard with multi-tab analytics

## Features

- **Real-time Data**: Fetches top 50 cryptocurrencies with market metrics
- **Dimensional Modeling**: Star schema with fact and dimension tables
- **Data Quality**: Validation, error handling, and data quality checks
- **Automated Orchestration**: Prefect-managed workflows with dependency tracking
- **Business Intelligence**: Pre-built analytics views for performance analysis
- **Interactive Dashboard**: Multi-tab Streamlit interface with filtering and visualizations

## Tech Stack

- **Python**: Core ETL logic and data processing
- **PostgreSQL**: Data warehouse and storage
- **dbt**: Data transformation and dimensional modeling
- **Prefect**: Workflow orchestration and logging/monitoring
- **Streamlit**: Interactive dashboard and visualization
- **Plotly**: Charting and data visualization
- **SQLAlchemy**: Database connectivity and ORM
