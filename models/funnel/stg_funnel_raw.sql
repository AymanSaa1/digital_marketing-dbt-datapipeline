{{
    config(
        materialized='table'
    )
}}
select * 
from {{ source('funnel_data', 'FUNNEL_DATA') }}