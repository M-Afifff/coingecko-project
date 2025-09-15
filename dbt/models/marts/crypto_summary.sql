-- dbt/models/marts/crypto_summary.sql 

{{ config(materialized='table') }}

WITH latest_raw AS (
    SELECT *
    FROM {{ ref('stg_crypto_prices') }}
    WHERE extracted_date = (
        SELECT MAX(extracted_date) 
        FROM {{ ref('stg_crypto_prices') }}
    )
),

latest_metrics AS (
    SELECT *
    FROM {{ ref('int_crypto_metrics') }}
    WHERE extracted_date = (
        SELECT MAX(extracted_date) 
        FROM {{ ref('int_crypto_metrics') }}
    )
)

SELECT 
    -- Identifiers
    r.crypto_id,
    r.symbol,
    r.name,
    
    -- Latest raw values
    r.current_price as latest_price,
    r.market_cap_billions as latest_market_cap_billions,
    r.rank as latest_rank,
    r.price_category,
    r.price_change_24h as latest_price_change_24h,
    r.volume_24h as latest_volume_24h,
    
    -- Classifications
    COALESCE(m.performance_24h, 'Unknown') as performance_24h,
    COALESCE(m.market_tier, 'Other') as market_tier,
    COALESCE(m.volatility_tier, 'Unknown') as volatility_tier,
    
    -- Data quality check
    CASE 
        WHEN m.record_count > 1 THEN 'Multiple Records'
        ELSE 'Single Record'
    END as data_quality_flag,
    
    -- Timestamps
    r.extracted_date,
    r.extracted_at as last_updated,
    
    -- Foreign Keys
    {{ dbt_utils.generate_surrogate_key(['r.crypto_id']) }} as crypto_sk,
    CASE 
        WHEN r.rank <= 10 THEN 1
        WHEN r.rank <= 50 THEN 2
        WHEN r.rank <= 100 THEN 3
        ELSE 4
    END as market_tier_sk,
    CASE 
        WHEN r.current_price < 0.01 THEN 1
        WHEN r.current_price < 1.00 THEN 2
        WHEN r.current_price < 100.00 THEN 3
        WHEN r.current_price < 10000.00 THEN 4
        ELSE 5
    END as price_category_sk,
    CASE 
        WHEN r.price_change_24h > 5 THEN 1
        WHEN r.price_change_24h > 0 THEN 2
        WHEN r.price_change_24h > -5 THEN 3
        ELSE 4
    END as performance_sk
    
FROM latest_raw r
LEFT JOIN latest_metrics m 
    ON r.crypto_id = m.crypto_id 
    AND r.extracted_date = m.extracted_date
    
ORDER BY r.rank ASC