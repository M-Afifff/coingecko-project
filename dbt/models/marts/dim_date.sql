-- dbt/models/marts/dim_date.sql

{{ config(materialized='table') }}

WITH date_range AS (
    SELECT 
        date_actual
    FROM (
        -- Generate date series from 2020 to 2030
        SELECT 
            DATE '2020-01-01' + (n || ' day')::INTERVAL as date_actual
        FROM generate_series(0, (DATE '2030-12-31' - DATE '2020-01-01')::INTEGER) n
    ) dates
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['date_actual']) }} as date_sk,
    date_actual,
    EXTRACT(DOW FROM date_actual) as day_of_week,
    TO_CHAR(date_actual, 'Day') as day_name,
    EXTRACT(WEEK FROM date_actual) as week_of_year,
    EXTRACT(MONTH FROM date_actual) as month_number,
    TO_CHAR(date_actual, 'Month') as month_name,
    EXTRACT(QUARTER FROM date_actual) as quarter,
    EXTRACT(YEAR FROM date_actual) as year,
    CASE WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday, -- Can be enhanced with holiday logic later
    EXTRACT(YEAR FROM date_actual) as fiscal_year,
    EXTRACT(QUARTER FROM date_actual) as fiscal_quarter
    
FROM date_range
ORDER BY date_actual