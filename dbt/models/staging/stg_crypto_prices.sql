-- Staging layer
{{ config(materialized='view') }}

SELECT 
    -- Identifiers
    crypto_id,
    symbol,
    name,
    
    -- Price data
    current_price,
    market_cap,
    rank,
    volume_24h,
    price_change_24h,
    circulating_supply,
    
    -- Calculated fields
    market_cap_billions,
    price_category,
    
    -- Timestamps
    last_updated,
    extracted_date,
    extracted_at,
    
    -- Metadata
    id as row_id
    
FROM crypto_prices

-- Basic filters
WHERE current_price > 0 
  AND market_cap > 0
  AND crypto_id IS NOT NULL
  AND symbol IS NOT NULL
