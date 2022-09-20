{{
    config(
        materialized='incremental',
        unique_key=['city', 'member_id'],
        incremental_strategy='merge'
    )
}}
select * from members_deltas