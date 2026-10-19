"""
Semana 2 - Prática: Transformar o dado da Semana 1 em Star Schema
(DIM_MOEDA + DIM_TEMPO + FATO_COTACAO)

Reescrito no padrão TaskFlow do Airflow: @dag no escopo do módulo + @task.

A lógica de extração (Semana 1) e a modelagem dimensional (Semana 2) são as
mesmas do script original. Mudança estrutural: as tasks não passam DataFrames
entre si — salvam Parquet intermediários em DATA_DIR e trocam apenas CAMINHOS
via XCom (o backend padrão serializa JSON; DataFrame não é serializável).
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

API_URL = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL"
# Pasta dentro do bind mount de dags/ — os arquivos aparecem no host em
# ~/docker/airflow-local/dags/data
DATA_DIR = Path("/opt/airflow/dags/data")


@dag(
    dag_id="api_requests",
    description="Cotações USD/EUR/BTC -> star schema (DIM_MOEDA, DIM_TEMPO, FATO_COTACAO)",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["semana-2", "star-schema"],
)
def api_requests():
    @task
    def extrair_dados_api(url: str) -> dict:
        """Semana 1: requisição à API. Retorna dict JSON — serializável via XCom."""
        logger.info("Iniciando requisição à API: %s", url)
        try:
            resposta = requests.get(url, timeout=10)
            resposta.raise_for_status()
        except requests.exceptions.RequestException as erro:
            logger.error("Falha ao consultar a API: %s", erro)
            raise
        logger.info("Requisição concluída com status %s", resposta.status_code)
        return resposta.json()

    @task
    def transformar_em_dataframe(dados_json: dict) -> str:
        """Semana 1: df flat. Salva em Parquet e devolve o caminho (XCom)."""
        logger.info("Transformando resposta JSON em DataFrame")
        df = pd.DataFrame(dados_json.values())
        logger.info("DataFrame criado com %d linhas e %d colunas", *df.shape)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        caminho = DATA_DIR / "flat.parquet"
        df.to_parquet(caminho, index=False)
        logger.info("df flat salvo em: %s", caminho)
        return str(caminho)

    @task
    def criar_dim_moeda(caminho_flat: str) -> str:
        """Semana 2: DIM_MOEDA com surrogate key própria (id_moeda)."""
        logger.info("Criando DIM_MOEDA")
        df = pd.read_parquet(caminho_flat)

        dim_moeda = (
            df[["code", "codein", "name"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_moeda.insert(0, "id_moeda", range(1, len(dim_moeda) + 1))

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        caminho = DATA_DIR / "dim_moeda.parquet"
        dim_moeda.to_parquet(caminho, index=False)
        logger.info(
            "DIM_MOEDA criada com %d linhas — salva em: %s", len(dim_moeda), caminho
        )
        return str(caminho)

    @task
    def criar_dim_tempo(caminho_flat: str) -> str:
        """Semana 2: DIM_TEMPO a partir de create_date, com id_tempo."""
        logger.info("Criando DIM_TEMPO")
        df = pd.read_parquet(caminho_flat)

        # create_date vem como texto, ex: "2026-08-25 09:35:32" — convertemos
        datas = pd.to_datetime(df["create_date"]).drop_duplicates().reset_index(drop=True)
        dim_tempo = pd.DataFrame(
            {
                "data_completa": datas,
                "ano": datas.dt.year,
                "mes": datas.dt.month,
                "dia": datas.dt.day,
                "hora": datas.dt.hour,
                "minuto": datas.dt.minute,
            }
        )
        dim_tempo.insert(0, "id_tempo", range(1, len(dim_tempo) + 1))

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        caminho = DATA_DIR / "dim_tempo.parquet"
        dim_tempo.to_parquet(caminho, index=False)
        logger.info(
            "DIM_TEMPO criada com %d linhas — salva em: %s", len(dim_tempo), caminho
        )
        return str(caminho)

    @task
    def criar_fato_cotacao(
        caminho_flat: str, caminho_dim_moeda: str, caminho_dim_tempo: str
    ) -> str:
        """Semana 2: FATO_COTACAO = chaves das dimensões + medidas numéricas."""
        logger.info("Criando FATO_COTACAO")
        df = pd.read_parquet(caminho_flat)
        dim_moeda = pd.read_parquet(caminho_dim_moeda)
        dim_tempo = pd.read_parquet(caminho_dim_tempo)

        df = df.copy()
        df["create_date"] = pd.to_datetime(df["create_date"])

        # merge com dim_moeda: casa pelas 3 colunas descritivas, traz id_moeda
        fato = df.merge(dim_moeda, on=["code", "codein", "name"], how="left")

        # merge com dim_tempo: casa pela data completa, traz id_tempo
        fato = fato.merge(
            dim_tempo, left_on="create_date", right_on="data_completa", how="left"
        )

        # só chaves estrangeiras + medidas — nenhum texto descritivo aqui
        colunas_fato = [
            "id_moeda", "id_tempo",
            "bid", "ask", "high", "low", "varBid", "pctChange",
        ]
        fato_cotacao = fato[colunas_fato].copy()

        # colunas vêm como texto da API (ex: "5.4050") — convertendo para número
        colunas_numericas = ["bid", "ask", "high", "low", "varBid", "pctChange"]
        fato_cotacao[colunas_numericas] = fato_cotacao[colunas_numericas].apply(
            pd.to_numeric, errors="coerce"
        )

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        caminho = DATA_DIR / "fato_cotacao.parquet"
        fato_cotacao.to_parquet(caminho, index=False)
        logger.info(
            "FATO_COTACAO criada com %d linhas e %d colunas — salva em: %s",
            *fato_cotacao.shape, caminho,
        )
        return str(caminho)

    @task
    def resumo(
        caminho_dim_moeda: str, caminho_dim_tempo: str, caminho_fato: str
    ) -> None:
        """Conferência visual: lê os 3 parquets e registra as tabelas no log."""
        dim_moeda = pd.read_parquet(caminho_dim_moeda)
        dim_tempo = pd.read_parquet(caminho_dim_tempo)
        fato_cotacao = pd.read_parquet(caminho_fato)

        logger.info("Star schema criado com sucesso.")
        logger.info("\n--- DIM_MOEDA ---\n%s", dim_moeda)
        logger.info("\n--- DIM_TEMPO ---\n%s", dim_tempo)
        logger.info("\n--- FATO_COTACAO ---\n%s", fato_cotacao)

    # Grafo de dependências — chamar as tasks É o que constrói o DAG
    dados_json = extrair_dados_api(API_URL)
    caminho_flat = transformar_em_dataframe(dados_json)
    caminho_dim_moeda = criar_dim_moeda(caminho_flat)
    caminho_dim_tempo = criar_dim_tempo(caminho_flat)
    caminho_fato = criar_fato_cotacao(caminho_flat, caminho_dim_moeda, caminho_dim_tempo)
    resumo(caminho_dim_moeda, caminho_dim_tempo, caminho_fato)


# A chamada no escopo do módulo é o que expõe o DAG ao parser —
# sem ela, o arquivo parseia sem erro e não registra DAG nenhum.
api_requests()

