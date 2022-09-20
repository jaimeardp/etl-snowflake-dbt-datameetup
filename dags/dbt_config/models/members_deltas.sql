{{ config(materialized='table') }}
select
	city ,
	member_id,
	min(joined) as joined
from
	members m
group by
	1,
	2