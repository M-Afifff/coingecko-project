-- Marts layer
{{ config(materialized='table') }}

SELECT 
    crypto_id,
    symbol,
    name,
    extracted_date,
    
    -- Price metrics
    ROUND(avg_price::numeric, 4) as avg_price,
    ROUND(min_price::numeric, 4) as min_price,
    ROUND(max_price::numeric, 4) as max_price,
    ROUND(price_volatility::numeric, 2) as price_volatility,
    
    -- Market metrics
    ROUND(avg_market_cap_billions::numeric, 2) as avg_market_cap_billions,
    ROUND(avg_volume_24h::numeric, 0) as avg_volume_24h,
    ROUND(avg_price_change_24h::numeric, 2) as avg_price_change_24h,
    
    -- Classifications
    best_rank,
    dominant_price_category,
    performance_24h,
    market_tier,
    volatility_tier,
    
    -- Metadata
    record_count,
    last_updated
    
FROM {{ ref('int_crypto_metrics') }}
WHERE extracted_date >= CURRENT_DATE - INTERVAL '30 days'