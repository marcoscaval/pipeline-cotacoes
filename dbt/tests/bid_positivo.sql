-- falha se existir cotação com bid <= 0
select * from {{ref('fato_cotacao')}} where bid <= 0
