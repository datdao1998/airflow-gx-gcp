{{ 
    config(
        materialized='incremental',
        unique_key='age',
        incremental_strategy='merge'
    ) 
}}

select 
    age,
    count(*) as number_of_records
from {{ source('CUSTOMER', 'customer') }}
group by age