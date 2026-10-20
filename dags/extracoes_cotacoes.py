import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL"
DATA_DIR = Path("/opt/airflow/data")
DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

def extrair_dados_api(url: str)	-> dict:
    logger.info("Iniciando requisição à API: %s", url)
    resposta = requests.get(url, timeout=10)
    resposta.raise_for_status()
    return resposta.json()


def transformar_em_dataframe(dados_json: dict) -> pd.DataFrame:
    return pd.DataFrame(dados_json.values())


def salvar_dados(df: pd.DataFrame, pasta_destino: Path)	-> None:
    pasta_destino.mkdir(parents=True, exist_ok=True)
    df.to_parquet(pasta_destino / "cotacoes.parquet", index=False)
    logger.info("Arquivo salvo com sucesso.")


def rodar_pipeline_extracao():
    dados_json = extrair_dados_api(API_URL)
    df = transformar_em_dataframe(dados_json)
    salvar_dados(df,DATA_DIR)


with DAG(
   dag_id="extracao_cotacoes",
   description="Extrai cotações e materializa o start schema via dbt",
   start_date=datetime(2026, 1, 1),
   schedule="@daily",
   catchup=False,
   tags=["projeto1", "raw", "dbt"],
) as dag:


    tarefa_extracao = PythonOperator(
       task_id="extrair_dados_api",
       python_callable=rodar_pipeline_extracao,
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run {DBT_ARGS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test {DBT_ARGS}")

    tarefa_extracao >> dbt_run >> dbt_test

