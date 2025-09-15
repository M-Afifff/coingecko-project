-- dbt/models/marts/dim_market_tier.sql

{{ config(materialized='table') }}

SELECT * FROM (
    VALUES 
        (1, 'TOP_10', 'Top 10', 'Top 10 cryptocurrencies by market cap - Blue chip investments', 1, 10, 'Low', 'Blue Chip', 1),
        (2, 'TOP_50', 'Top 50', 'Top 11-50 cryptocurrencies - Established projects', 11, 50, 'Medium', 'Growth', 2),
        (3, 'TOP_100', 'Top 100', 'Top 51-100 cryptocurrencies - Growth potential', 51, 100, 'Medium-High', 'Growth', 3),
        (4, 'OTHER', 'Other', 'Cryptocurrencies ranked 100+ - High risk/reward', 101, 9999, 'High', 'Speculative', 4)
) AS t(
    market_tier_sk, 
    tier_code, 
    tier_name, 
    tier_description, 
    min_rank, 
    max_rank, 
    risk_level, 
    investment_type, 
    tier_order
)