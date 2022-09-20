-- Creación de tablas físicas auxiliares
-- Paso 1. Usuarios sin duplicados y con el campo ciudad

create table miesquema.members_usuarios_ciudad_aux1 as
select
	city ,
	member_id,
	min(joined) as joined
from
	miesquema.members m
group by
	1,
	2
;
-- Verificar la tabla recién creada

select * from miesquema.members_usuarios_ciudad_aux1 limit 10;

-- Paso 2. Año y mes

create table miesquema.members_usuarios_ciudad_aux2 as
select
	city,
	member_id,
	joined,
	extract(year from joined) as anio,
	extract(month from joined) as mes
from
	miesquema.members_usuarios_ciudad_aux1 muca ;

-- Verificar la tabla recién creada

select * from miesquema.members_usuarios_ciudad_aux2 limit 10;

-- Paso 3 .Usuarios por ciudad y año

create table miesquema.members_usuarios_ciudad_aux3 as 
select
	city,
	anio,
	count(member_id)
from 
	miesquema.members_usuarios_ciudad_aux2
group by 
	1,
	2
order by 
	1,
	2
	;

-- Verificar la tabla recién creada

select * from miesquema.members_usuarios_ciudad_aux3 limit 10;

-- Paso 4. Graficamos.

-- Paso 4.1 Copiar los datos.

-- Paso 4.2 Ingrese a RAWGraphs, pege los datos.

-- Paso 4.3 Seleccione el tipo de gráfico que le interesa.

-- Paso 4.4 Arrastre las dimensiones a los campos que considera.

-- Paso 4.5 Analice los resultados.

-- Tablas temporales
-- El siguiente método es muy similar al ejercicio anterior. 
-- La diferencia es que ahora vamos a utilizar tablas temporales que quedan en un esquema temporal y 
-- son eliminadas cuando acabamos la sesión, para esto usamos el comando “create temporary table“.

-- Paso 1. Crear la tabla con el campo city sin duplicados.

create temporary table members_aux1_filtros as
select
	city ,
	member_id,
	min(joined) as joined
from
	miesquema.members m
group by
	1,
	2
;
-- Paso 2. Crear los campos mes y año, usando la tabla sin duplicados y con el campo city.

create temporary table members_aux2_filtros as
select
	city,
	member_id,
	joined,
	extract(year from joined) as anio,
	extract(month from joined) as mes
from
	members_aux1_filtros maf ;

-- Paso 3. Contar el número de clientes por ciudad al año.

create temporary table members_aux3_filtros as 
select
	city,
	anio,
	count(member_id)
from 
	members_aux2_filtros
group by 
	1,
	2
	;

-- Sub-queries
-- En seguida construimos el mismo indicador “usuarios anuales por ciudad” usando sub-queries. 
-- Que consisten en queries dentro de otros queries. En vez de llamar a una tabla con la sentencia “from“, 
-- se llama a un query. Tenemos la ventaja que podemos hacer todo en un único script.

select
	country,
	state,
	city,
	anio,
	count(member_id)
from
	(
	select
		country,
		state,
		city,
		member_id,
		joined,
		extract(year from joined) as anio,
		extract(month from joined) as mes
	from
		(
		select
			country,
			state,
			city,
			member_id,
			min(joined) as joined
		from
			miesquema.members m
		group by
			1,2,3,4
			) x) y
where state not in ('ca') and city not in ('Chicago Park')
group by 
	1,
	2,
	3,
	4
order by 
	1,
	2,
	3,
	4

-- Sentencia with
-- La sentencia with permite crear tablas temporales dentro de un mismo query combinando conceptos 
-- de tablas temporales y subquerys, pero en este caso se nombran y definen al 
-- principio del query (tantos como queramos) y luego se llaman con la sentencia from al final del query.

create table zzz_growth_yoy_filtros_geo as
select
	country,
	state,
	city,
	anio,
	freq,
	freqt1,
	(freq / freqt1-1) as growth_yoy
from
	(
	select
		*,
		lag(freq, 1) over (order by freq) as freqt1
	from
		(
		select
			country,
			state,
			city,
			anio,
			count(member_id) freq
		from
			(
			select
				country,
				state,
				city,
				member_id,
				joined,
				extract (year from joined) as anio
			from
				(
				select
					country,
					state,
					city,
					member_id,
					min(joined) as joined
				from
					miesquema.members m
				group by
					member_id) x) y
		group by
			1,
			2,
			3,
			4 ) z
	order by
		1,
		2,
		3,
		4 ) a

-- Y listo, tenemos nuestra tabla para trabajarla en nuestra herramienta de visualización favorita.