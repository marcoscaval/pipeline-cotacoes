select distinct
	md5(moeda_origem ||'-'||moeda_destino) as moeda_sk,
	moeda_origem,
	moeda_destino,
	nome_par
from {{ref('stg_cotacoes')}}

