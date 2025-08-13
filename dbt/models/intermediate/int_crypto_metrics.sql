-- Intermediate layer
{{ config(materialized='view') }}

WITH daily_aggregations AS (
    SELECT 
        crypto_id,
        symbol,
        name,
        extracted_date,
        
        -- Price metrics
        AVG(current_price) as avg_price,
        MIN(current_price) as min_price,
        MAX(current_price) as max_price,
        STDDEV(current_price) as price_volatility,
        
        -- Market metrics
        AVG(market_cap_billions) as avg_market_cap_billions,
        AVG(volume_24h) as avg_volume_24h,
        AVG(price_change_24h) as avg_price_change_24h,
        
        -- Rankings and categories
        MIN(rank) as best_rank,
        MODE() WITHIN GROUP (ORDER BY price_category) as dominant_price_category,
        
        -- Data quality metrics
        COUNT(*) as record_count,
        COUNT(CASE WHEN current_price IS NULL THEN 1 END) as null_price_count,
        
        -- Metadata
        MAX(extracted_at) as last_updated
        
    FROM {{ ref('stg_crypto_prices') }}
    WHERE extracted_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY crypto_id, symbol, name, extracted_date
),

enriched_metrics AS (
    SELECT 
        *,
        
        -- Performance indicators
        CASE 
            WHEN avg_price_change_24h > 5 THEN 'Strong Gain'
            WHEN avg_price_change_24h > 0 THEN 'Gain'
            WHEN avg_price_change_24h > -5 THEN 'Loss'
            ELSE 'Strong Loss'
        END as performance_24h,
        
        -- Market tier classification
        CASE 
            WHEN best_rank <= 10 THEN 'Top 10'
            WHEN best_rank <= 50 THEN 'Top 50'
            WHEN best_rank <= 100 THEN 'Top 100'
            ELSE 'Other'
        END as market_tier,
        
        -- Volatility classification
        CASE 
            WHEN price_volatility < 100 THEN 'Low Volatility'
            WHEN price_volatility < 500 THEN 'Medium Volatility'
            ELSE 'High Volatility'
        END as volatility_tier
        
    FROM daily_aggregations
)

SELECT * FROM enriched_metrics
ORDER BY extracted_date DESC, avg_market_cap_billions DESC