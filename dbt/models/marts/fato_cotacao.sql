select
	m.moeda_sk,
	t.tempo_sk,
	s.bid, s.ask, s.high, s.low, s.variacao_pct
from {{ref('stg_cotacoes')}} s
join {{ref('dim_moeda')}} m
	on s.moeda_origem = m.moeda_origem and s.moeda_destino = m.moeda_destino
join {{ref('dim_tempo')}} t
on t.cotado_em = s.cotado_em

