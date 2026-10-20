select
    code as	moeda_origem,
    codein as moeda_destino,
    name   as nome_par,
    try_cast(bid as	double) as bid,
    try_cast(ask	as	double)	as ask,
    try_cast(high	as	double) as high,
    try_cast(low	as	double) as low,
    try_cast("pctChange" as	double)	as variacao_pct,
    cast(to_timestamp(try_cast("timestamp" as bigint)) as timestamp) as cotado_em
from {{source('raw','cotacoes')	}}

