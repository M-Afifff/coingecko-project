-- dbt/models/marts/dim_price_category.sql

{{ config(materialized='table') }}

SELECT * FROM (
    VALUES 
        (1, 'MICRO', 'Micro Cap', 'Under $0.01 - Typically newer tokens or high-supply cryptos', 0.00000001, 0.01, 'High', 'Speculative', 1),
        (2, 'LOW', 'Low Price', '$0.01 - $1.00 - Accessible for retail investors', 0.01, 1.00, 'High', 'Retail Friendly', 2),
        (3, 'MEDIUM', 'Medium Price', '$1.00 - $100.00 - Established altcoins', 1.00, 100.00, 'Medium', 'Balanced', 3),
        (4, 'HIGH', 'High Price', '$100.00 - $10,000 - Major cryptocurrencies', 100.00, 10000.00, 'Low', 'Institutional', 4),
        (5, 'PREMIUM', 'Premium Price', 'Over $10,000 - Elite cryptocurrencies like Bitcoin', 10000.00, 999999999.99, 'Low', 'Store of Value', 5)
) AS t(
    price_category_sk, 
    category_code, 
    category_name, 
    category_description, 
    min_price_usd, 
    max_price_usd, 
    volatility_expectation, 
    investor_type, 
    category_order
)