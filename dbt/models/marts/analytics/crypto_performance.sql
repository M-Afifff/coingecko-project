-- dbt/models/marts/analytics/crypto_performance.sql

{{ config(materialized='view') }}

SELECT 
    -- Dimension attributes
    dc.symbol,
    dc.name,
    dc.category,
    dd.date_actual,
    dd.month_name,
    dd.quarter,
    mt.tier_name,
    mt.risk_level,
    pc.category_name as price_range,
    perf.performance_name,
    perf.signal_type,
    
    -- Fact measures
    cd.avg_price,
    cd.avg_market_cap_billions,
    cd.avg_volume_24h,
    cd.avg_price_change_24h,
    cd.price_volatility,
    cd.best_rank,
    
    -- Calculated measures
    CASE 
        WHEN cd.avg_volume_24h > cd.avg_market_cap_billions * 1000000000 * 0.1 
        THEN 'High Liquidity' 
        ELSE 'Normal Liquidity' 
    END as liquidity_status,
    
    ROUND(
        cd.avg_price * 
        POWER(1 + (cd.avg_price_change_24h / 100), 7), 
        4
    ) as projected_weekly_price
    
FROM {{ ref('crypto_daily') }} cd
JOIN {{ ref('dim_crypto') }} dc ON cd.crypto_sk = dc.crypto_sk
JOIN {{ ref('dim_date') }} dd ON cd.date_sk = dd.date_sk
JOIN {{ ref('dim_market_tier') }} mt ON cd.market_tier_sk = mt.market_tier_sk
JOIN {{ ref('dim_price_category') }} pc ON cd.price_category_sk = pc.price_category_sk
JOIN {{ ref('dim_performance') }} perf ON cd.performance_sk = perf.performance_sk

WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '30 days'
  AND dc.is_active = TRUE
  
ORDER BY dd.date_actual DESC, cd.best_rank ASC