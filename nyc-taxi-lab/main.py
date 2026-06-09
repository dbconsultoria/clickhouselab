# NYC Taxi ClickHouse Lab — Backend
# Autor: Rodrigo Ribeiro — https://www.linkedin.com/in/rodrigo-ribeiro-pro/
#
# Como executar:
#   1. docker compose up -d
#   2. pip install -r requirements.txt
#   3. uvicorn main:app --reload
#   4. Abrir http://localhost:8000

import asyncio                          # gerenciamento de tarefas assíncronas
import gzip                             # leitura de arquivos comprimidos .gz
from datetime import datetime           # conversão de strings de data para objetos datetime
import re                               # expressões regulares (detecção de LIMIT nas queries)
import tempfile                         # diretório temporário do sistema operacional
import time                             # medição de tempo decorrido
from dataclasses import dataclass       # criação de classes de dados simples
from pathlib import Path                # manipulação de caminhos de arquivos
from typing import Optional             # tipagem opcional para campos que podem ser None

import clickhouse_connect               # cliente oficial Python para o ClickHouse
import httpx                            # cliente HTTP assíncrono para download do dataset
from fastapi import FastAPI, HTTPException          # framework web e tratamento de erros HTTP
from fastapi.responses import FileResponse, JSONResponse  # respostas de arquivo e JSON
from fastapi.staticfiles import StaticFiles         # servir arquivos estáticos (frontend)
from pydantic import BaseModel          # validação e tipagem do corpo das requisições

# URL do dataset NYC Taxi hospedado no S3 da ClickHouse
DATASET_URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz"

# Caminho local onde o arquivo .gz será salvo (usa o temp do sistema operacional)
LOCAL_PATH = Path(tempfile.gettempdir()) / "trips_0.gz"

# Número de linhas enviadas ao ClickHouse por vez durante o insert
BATCH_SIZE = 50_000

# Limite máximo de linhas retornadas por query no endpoint /query
RESULT_LIMIT = 500

# Instância principal da aplicação FastAPI
app = FastAPI()


@dataclass
class PipelineState:
    # Fase atual do pipeline: idle, downloading, inserting ou ready
    phase: str = "idle"

    # Percentual concluído do download (0.0 a 100.0)
    download_progress: float = 0.0

    # Percentual concluído do insert (0.0 a 100.0)
    insert_progress: float = 0.0

    # Tempo decorrido em segundos desde o início do insert
    insert_elapsed_seconds: float = 0.0

    # Quantidade de linhas já inseridas no ClickHouse
    rows_inserted: int = 0

    # Total de linhas no dataset (preenchido ao final do insert)
    total_rows: int = 0

    # Mensagem de erro caso o pipeline falhe (None se não houver erro)
    error: Optional[str] = None


# Objeto de estado global compartilhado entre os endpoints e o pipeline
state = PipelineState()


# ---------------------------------------------------------------------------
# Helpers do pipeline
# ---------------------------------------------------------------------------

# Tamanho exato do arquivo comprimido em bytes (Content-Length do S3)
DATASET_SIZE = 81_887_950


async def run_download() -> None:
    # Verifica se o arquivo já foi baixado completamente; se sim, pula o download
    if LOCAL_PATH.exists() and LOCAL_PATH.stat().st_size == DATASET_SIZE:
        state.phase = "downloading"        # mantém a fase visível no frontend
        state.download_progress = 100.0    # marca como 100% para o frontend avançar
        return                             # sai da função sem baixar nada

    state.phase = "downloading"            # atualiza a fase para o frontend exibir a barra
    state.download_progress = 0.0         # reinicia o progresso

    # Abre conexão HTTP com streaming; follow_redirects lida com redirecionamentos do S3
    async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
        async with client.stream("GET", DATASET_URL) as response:
            response.raise_for_status()                                    # lança erro se HTTP != 2xx
            total = int(response.headers.get("content-length", 0))        # tamanho total em bytes
            received = 0                                                   # contador de bytes recebidos

            with LOCAL_PATH.open("wb") as f:                              # abre o arquivo para escrita binária
                async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):  # lê em chunks de 1 MB
                    f.write(chunk)                                         # grava o chunk no disco
                    received += len(chunk)                                 # acumula bytes recebidos
                    if total:                                              # evita divisão por zero
                        state.download_progress = round(received / total * 100, 1)  # atualiza percentual
                    await asyncio.sleep(0)                                 # cede controle ao event loop

    state.download_progress = 100.0       # garante 100% ao finalizar


def _get_clickhouse_client():
    # Cria e retorna um cliente HTTP conectado ao ClickHouse local sem senha
    return clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")


def _prepare_schema(client) -> None:
    # Cria o banco de dados nyc_taxi caso ainda não exista
    client.command("CREATE DATABASE IF NOT EXISTS nyc_taxi")

    # Cria a tabela trips com os tipos otimizados para consultas analíticas
    client.command("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.trips (
            trip_id             UInt32,                  -- identificador único da corrida
            pickup_datetime     DateTime,                -- data e hora do embarque
            dropoff_datetime    DateTime,                -- data e hora do desembarque
            pickup_longitude    Float32,                 -- longitude do ponto de embarque
            pickup_latitude     Float32,                 -- latitude do ponto de embarque
            dropoff_longitude   Float32,                 -- longitude do ponto de desembarque
            dropoff_latitude    Float32,                 -- latitude do ponto de desembarque
            passenger_count     UInt8,                   -- número de passageiros
            trip_distance       Float32,                 -- distância percorrida em milhas
            fare_amount         Float32,                 -- valor da tarifa em dólares
            tip_amount          Float32,                 -- valor da gorjeta em dólares
            total_amount        Float32,                 -- valor total cobrado em dólares
            payment_type        LowCardinality(String),  -- forma de pagamento (CSH, CRE, etc.)
            pickup_ntaname      LowCardinality(String),  -- bairro de embarque
            dropoff_ntaname     LowCardinality(String)   -- bairro de desembarque
        ) ENGINE = MergeTree()
        ORDER BY (pickup_datetime, trip_id)              -- chave de ordenação primária
    """)


# Lista ordenada das colunas que serão inseridas (deve coincidir com o TSV)
_COLUMNS = [
    "trip_id", "pickup_datetime", "dropoff_datetime",
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
    "passenger_count", "trip_distance",
    "fare_amount", "tip_amount", "total_amount",
    "payment_type", "pickup_ntaname", "dropoff_ntaname",
]

# Conjuntos de colunas por tipo — usados na função de coerção abaixo
_UINT32   = {"trip_id"}                  # inteiro sem sinal de 32 bits
_UINT8    = {"passenger_count"}          # inteiro sem sinal de 8 bits (0–255)
_FLOAT32  = {"pickup_longitude", "pickup_latitude", "dropoff_longitude",
             "dropoff_latitude", "trip_distance", "fare_amount",
             "tip_amount", "total_amount"}   # ponto flutuante de 32 bits
_DATETIME = {"pickup_datetime", "dropoff_datetime"}  # data e hora


def _coerce(col: str, val: str):
    val = val.strip()  # remove espaços e quebras de linha ao redor do valor
    try:
        if col in _UINT32:
            return int(val) if val else 0                                           # converte para inteiro ou zero
        if col in _UINT8:
            return int(float(val)) if val else 0                                    # passa por float para lidar com "1.0"
        if col in _FLOAT32:
            return float(val) if val else 0.0                                       # converte para ponto flutuante
        if col in _DATETIME:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S") if val else datetime(1970, 1, 1)  # parse da data
    except (ValueError, TypeError):
        # Valores inválidos recebem o zero/epoch correspondente ao tipo
        if col in (_UINT32 | _UINT8):
            return 0
        if col in _FLOAT32:
            return 0.0
        if col in _DATETIME:
            return datetime(1970, 1, 1)    # epoch Unix como fallback para datas inválidas
    return val                             # strings (payment_type, bairros) retornam sem conversão


async def run_insert() -> None:
    state.phase = "inserting"              # atualiza fase para o frontend exibir a barra de insert
    state.insert_progress = 0.0           # reinicia percentual do insert
    state.rows_inserted = 0              # reinicia contador de linhas

    loop = asyncio.get_running_loop()     # obtém o event loop ativo (Python 3.10+)
    gz_size = LOCAL_PATH.stat().st_size   # tamanho do arquivo comprimido para estimar progresso

    # Cria o cliente e prepara o schema em threads separadas para não bloquear o event loop
    client = await loop.run_in_executor(None, _get_clickhouse_client)
    await loop.run_in_executor(None, _prepare_schema, client)

    start = time.monotonic()              # marca o início do insert para calcular elapsed

    def read_and_insert():
        rows_done = 0                     # contador local de linhas inseridas
        batch: list = []                  # buffer de linhas a ser enviado ao ClickHouse

        with LOCAL_PATH.open("rb") as raw_f:                              # abre o arquivo comprimido em modo binário
            with gzip.open(raw_f, "rt", encoding="utf-8", errors="replace") as gz:  # descomprime e decodifica
                header = [c.strip() for c in gz.readline().split("\t")]   # lê e parseia a linha de cabeçalho
                for line in gz:                                            # itera linha a linha pelo dataset
                    fields = line.rstrip("\n").split("\t")                 # divide cada linha pelo separador tab
                    raw_row = dict(zip(header, fields))                   # mapeia nome da coluna → valor
                    row = [_coerce(col, raw_row.get(col, "")) for col in _COLUMNS]  # converte tipos
                    batch.append(row)                                      # adiciona linha ao buffer

                    if len(batch) >= BATCH_SIZE:                          # quando o buffer atinge 50k linhas
                        client.insert("nyc_taxi.trips", batch, column_names=_COLUMNS)  # envia batch ao ClickHouse
                        rows_done += len(batch)                           # acumula total inserido
                        batch = []                                        # limpa o buffer

                        elapsed = time.monotonic() - start                # calcula tempo decorrido
                        state.rows_inserted = rows_done                   # atualiza estado para o frontend
                        state.insert_elapsed_seconds = round(elapsed, 1) # atualiza elapsed no estado
                        state.insert_progress = round(                    # estima % pelo avanço no arquivo comprimido
                            min(raw_f.tell() / gz_size * 100, 99.0), 1
                        )

                if batch:                                                  # insere o último batch (< 50k linhas)
                    client.insert("nyc_taxi.trips", batch, column_names=_COLUMNS)
                    rows_done += len(batch)

        state.rows_inserted = rows_done                                    # total final de linhas inseridas
        state.total_rows = rows_done                                       # registra total no estado
        state.insert_elapsed_seconds = round(time.monotonic() - start, 1) # tempo total do insert
        state.insert_progress = 100.0                                      # marca insert como concluído

    await loop.run_in_executor(None, read_and_insert)   # executa a leitura/insert em thread separada


def _file_ready() -> bool:
    # Retorna True se o arquivo já existe e tem pelo menos o tamanho esperado
    return LOCAL_PATH.exists() and LOCAL_PATH.stat().st_size >= DATASET_SIZE


async def pipeline() -> None:
    try:
        if _file_ready():
            state.phase = "downloading"        # exibe fase de download brevemente no frontend
            state.download_progress = 100.0    # arquivo já completo, pula para o insert
        else:
            await run_download()               # baixa o arquivo se não estiver disponível
        await run_insert()                     # insere os dados no ClickHouse
        state.phase = "ready"                  # sinaliza que o dataset está disponível para queries
    except Exception as exc:
        state.error = str(exc)                 # armazena a mensagem de erro no estado
        state.phase = "idle"                   # retorna à fase inicial para permitir nova tentativa


# ---------------------------------------------------------------------------
# Rotas da API
# ---------------------------------------------------------------------------

@app.get("/status")
async def get_status():
    # Retorna o estado atual do pipeline como JSON
    return {
        "phase": state.phase,
        "download_progress": state.download_progress,
        "insert_progress": state.insert_progress,
        "insert_elapsed_seconds": state.insert_elapsed_seconds,
        "rows_inserted": state.rows_inserted,
        "total_rows": state.total_rows,
        "error": state.error,
    }


@app.post("/start")
async def start_pipeline():
    # Impede iniciar o pipeline se ele já estiver em execução
    if state.phase not in ("idle", "ready"):
        raise HTTPException(status_code=400, detail=f"Pipeline already running (phase={state.phase})")

    # Reinicia todos os campos do estado antes de começar
    state.phase = "idle"
    state.download_progress = 0.0
    state.insert_progress = 0.0
    state.insert_elapsed_seconds = 0.0
    state.rows_inserted = 0
    state.total_rows = 0
    state.error = None

    asyncio.create_task(pipeline())   # dispara o pipeline como tarefa assíncrona em background
    return {"status": "started"}


@app.post("/reset")
async def reset_pipeline():
    # Não permite reset enquanto o pipeline estiver em execução
    if state.phase in ("downloading", "inserting"):
        raise HTTPException(status_code=400, detail="Pipeline is running — wait for it to finish")

    # Limpa todo o estado para o valor inicial
    state.phase = "idle"
    state.download_progress = 0.0
    state.insert_progress = 0.0
    state.insert_elapsed_seconds = 0.0
    state.rows_inserted = 0
    state.total_rows = 0
    state.error = None
    return {"status": "reset"}


class QueryRequest(BaseModel):
    sql: str   # query SQL recebida no corpo da requisição POST


@app.post("/query")
async def run_query(req: QueryRequest):
    sql = req.sql.strip().rstrip(";")   # remove espaços e ponto-e-vírgula final

    # Adiciona LIMIT automaticamente se a query não tiver um, para evitar respostas gigantes
    if not re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        sql = f"{sql} LIMIT {RESULT_LIMIT}"

    try:
        loop = asyncio.get_running_loop()   # obtém o event loop ativo

        def execute():
            ch = _get_clickhouse_client()           # cria cliente para esta query
            t0 = time.monotonic()                   # marca início para medir latência
            res = ch.query(sql)                     # executa a query no ClickHouse
            elapsed = (time.monotonic() - t0) * 1000  # calcula latência em milissegundos
            return res, elapsed

        res, elapsed_ms = await loop.run_in_executor(None, execute)   # executa em thread separada

        return {
            "columns": list(res.column_names),                        # nomes das colunas do resultado
            "rows": [list(row) for row in res.result_rows],           # linhas como listas de valores
            "elapsed_ms": round(elapsed_ms, 2),                       # latência arredondada em ms
            "row_count": len(res.result_rows),                        # quantidade de linhas retornadas
        }
    except Exception as exc:
        return JSONResponse(status_code=200, content={"error": str(exc)})   # retorna erro sem quebrar o frontend


@app.get("/")
async def serve_index():
    # Serve o arquivo HTML do frontend
    return FileResponse("static/index.html")


# Monta o diretório static para servir CSS, JS e outros assets futuros
app.mount("/static", StaticFiles(directory="static"), name="static")
