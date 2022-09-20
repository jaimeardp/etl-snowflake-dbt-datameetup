{{ config(materialized='table') }}
select
	city,
	anio,
	count(member_id) as len
from 
	members_usuarios_ciudad_aux2 m
group by 
	1,
	2
order by 
	1,
	2