-- dbt/models/marts/dim_performance.sql

{{ config(materialized='table') }}

SELECT * FROM (
    VALUES 
        (1, 'STRONG_GAIN', 'Strong Gain', 'Over +5% in 24h - Strong bullish momentum', 5.00, 999.99, 'Bullish', 'Consider Profit Taking', '#00C851', 1),
        (2, 'GAIN', 'Gain', '0% to +5% in 24h - Positive momentum', 0.00, 5.00, 'Bullish', 'Hold or Accumulate', '#4CAF50', 2),
        (3, 'LOSS', 'Loss', '-5% to 0% in 24h - Minor correction', -5.00, 0.00, 'Bearish', 'Monitor or Buy Dip', '#FF9800', 3),
        (4, 'STRONG_LOSS', 'Strong Loss', 'Under -5% in 24h - Strong bearish momentum', -999.99, -5.00, 'Bearish', 'Risk Assessment', '#F44336', 4)
) AS t(
    performance_sk, 
    performance_code, 
    performance_name, 
    performance_description, 
    min_change_pct, 
    max_change_pct, 
    signal_type, 
    trading_action, 
    color_code, 
    performance_order
)