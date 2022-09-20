{{ config(materialized='table') }}
select
	city,
	member_id,
	joined,
	extract(year from TO_DATE(joined)) as anio,
	extract(month from TO_DATE(joined)) as mes
from
	members_usuarios_ciudad_aux1 muca