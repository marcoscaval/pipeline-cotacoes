select	distinct
	cast(strftime(cotado_em,'%Y%m%d%H%M%S') as bigint) as tempo_sk,
	cotado_em,
	cast(cotado_em	as	date)		as	data,
	extract(year from	cotado_em)	as	ano,
	extract(month from	cotado_em)	as	mes,
	extract(day from	cotado_em)	as	dia,
	extract(hour from	cotado_em)	as	hora
from {{ref('stg_cotacoes')}}

