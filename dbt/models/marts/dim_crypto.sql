-- dbt/models/marts/dim_crypto.sql

{{ config(materialized='table') }}

WITH crypto_master AS (
    SELECT DISTINCT
        crypto_id,
        symbol,
        name,
        
        -- Categorize cryptocurrencies 
        CASE 
            WHEN crypto_id IN ('bitcoin', 'ethereum', 'binancecoin', 'cardano', 'solana') THEN 'Layer 1'
            WHEN crypto_id IN ('tether', 'usd-coin', 'binance-usd', 'dai', 'true-usd') THEN 'Stablecoin'
            WHEN crypto_id IN ('uniswap', 'chainlink', 'aave', 'compound', 'maker') THEN 'DeFi'
            WHEN crypto_id LIKE '%nft%' OR crypto_id IN ('enjincoin', 'theta-token') THEN 'NFT'
            WHEN crypto_id IN ('dogecoin', 'shiba-inu', 'pepe') THEN 'Meme'
            ELSE 'Other'
        END as category,
        
        -- Determine active/inactive
        CASE 
            WHEN MAX(extracted_date) >= CURRENT_DATE - INTERVAL '7 days' THEN TRUE
            ELSE FALSE
        END as is_active,
        
        MIN(extracted_date) as first_seen_date,
        MAX(extracted_date) as last_seen_date
        
    FROM {{ ref('stg_crypto_prices') }}
    GROUP BY crypto_id, symbol, name
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['crypto_id']) }} as crypto_sk,
    crypto_id,
    symbol,
    name,
    category,
    is_active,
    first_seen_date,
    last_seen_date as last_updated_date,
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM crypto_master
ORDER BY crypto_id