-- dbt/models/marts/crypto_daily.sql

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
    last_updated,
    
    -- Foreign Keys
    {{ dbt_utils.generate_surrogate_key(['crypto_id']) }} as crypto_sk,
    {{ dbt_utils.generate_surrogate_key(['extracted_date']) }} as date_sk,
    CASE 
        WHEN best_rank <= 10 THEN 1
        WHEN best_rank <= 50 THEN 2
        WHEN best_rank <= 100 THEN 3
        ELSE 4
    END as market_tier_sk,
    CASE 
        WHEN avg_price < 0.01 THEN 1
        WHEN avg_price < 1.00 THEN 2
        WHEN avg_price < 100.00 THEN 3
        WHEN avg_price < 10000.00 THEN 4
        ELSE 5
    END as price_category_sk,
    CASE 
        WHEN avg_price_change_24h > 5 THEN 1
        WHEN avg_price_change_24h > 0 THEN 2
        WHEN avg_price_change_24h > -5 THEN 3
        ELSE 4
    END as performance_sk
    
FROM {{ ref('int_crypto_metrics') }}
WHERE extracted_date >= CURRENT_DATE - INTERVAL '30 days'