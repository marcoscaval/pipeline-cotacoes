# Pipeline de Cotações

Pipeline de extração de cotações de moedas (USD, EUR, BTC) a partir de uma API pública, orquestrado via Airflow.

## Como rodar localmente

1. Suba o Airflow: Docker composse up -d
2. Acesse http://192.168.5.52:8080 (login: airflow / airflow)
3. Ative a DAG extracao_cotacoes


## Estrutura
- dags/ - DAGs do Airflow
- src/  - scripts de extração e	transformação
- data/	- saída	dos arquivos Parquet (gerado automaticamente)

## Arquitetura
![Diagrama de arquitetura](diagrama-arquitetura.png)
