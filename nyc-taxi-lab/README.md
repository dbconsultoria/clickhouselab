# NYC Taxi ClickHouse Lab

> Autor: **Rodrigo Ribeiro** — [linkedin.com/in/rodrigo-ribeiro-pro](https://www.linkedin.com/in/rodrigo-ribeiro-pro/)

Pipeline interativo para ingestão e consulta do dataset NYC Taxi (~1M linhas) usando ClickHouse como banco colunar. Interface web em tempo real acompanha o progresso do download e do insert, e permite executar queries SQL arbitrárias após a carga.

---

## Stack

| Camada | Tecnologia |
|---|---|
| Banco de dados | ClickHouse (Docker) — MergeTree engine |
| Backend | Python 3.11+ · FastAPI · Uvicorn |
| Cliente ClickHouse | clickhouse-connect (HTTP, porta 8123) |
| Download | httpx com streaming assíncrono |
| Frontend | HTML + CSS + JS puro (sem framework) |

---

## Estrutura do projeto

```
nyc-taxi-lab/
├── docker-compose.yml   # ClickHouse com volume persistente
├── requirements.txt     # Dependências Python
├── main.py              # Backend FastAPI (pipeline + API)
└── static/
    └── index.html       # Frontend single-page
```

---

## Dataset

- **Fonte:** [ClickHouse Sample Datasets](https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz)
- **Formato:** TSV comprimido em gzip (~78 MB comprimido, ~550 MB descomprimido)
- **Linhas:** ~1 milhão de corridas de táxi em Nova York (shard único; o dataset completo tem 4 arquivos `trips_0.gz`–`trips_3.gz` totalizando ~20M linhas)
- **Tabela:** `nyc_taxi.trips` com 15 colunas (datas, coordenadas, valores, bairros)

### Schema

```sql
CREATE TABLE nyc_taxi.trips (
    trip_id             UInt32,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    pickup_longitude    Float32,
    pickup_latitude     Float32,
    dropoff_longitude   Float32,
    dropoff_latitude    Float32,
    passenger_count     UInt8,
    trip_distance       Float32,
    fare_amount         Float32,
    tip_amount          Float32,
    total_amount        Float32,
    payment_type        LowCardinality(String),
    pickup_ntaname      LowCardinality(String),
    dropoff_ntaname     LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (pickup_datetime, trip_id);
```

---

## Performance observada

### Ingestão

| Etapa | Resultado |
|---|---|
| Download (~78 MB) | ~30s (depende da conexão) |
| Insert (1.000.660 linhas) | ~55s |
| Throughput Python → ClickHouse | ~18.000 linhas/seg |

O gargalo da ingestão é o pipeline Python (descompressão gzip + parse de datas), não o ClickHouse. Abordagens nativas como `INSERT INTO ... FROM S3(...)` atingem 5–10M linhas/seg.

### Queries analíticas

Medições reais sobre 1.000.660 linhas:

| Query | Tempo |
|---|---|
| `GROUP BY bairro + 2 agregações + ORDER BY` | **78ms** |
| `count(*)` simples | < 5ms |

Por que é rápido:

- **Armazenamento colunar** — lê apenas as colunas referenciadas na query
- **MergeTree** — dados ordenados em disco, leitura sequencial eficiente
- **Vetorização SIMD** — operações em blocos de valores do mesmo tipo
- **`LowCardinality(String)`** — bairros armazenados como dicionário interno; GROUP BY opera sobre inteiros

---

## API

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/` | Serve o frontend |
| `GET` | `/status` | Estado atual do pipeline (JSON) |
| `POST` | `/start` | Inicia o pipeline (download + insert) |
| `POST` | `/reset` | Reseta o estado para `idle` |
| `POST` | `/query` | Executa uma query SQL no ClickHouse |

### Exemplo — `/status`

```json
{
  "phase": "inserting",
  "download_progress": 100.0,
  "insert_progress": 42.3,
  "insert_elapsed_seconds": 23.1,
  "rows_inserted": 420000,
  "total_rows": 1000660,
  "error": null
}
```

### Exemplo — `/query`

**Request:**
```json
{ "sql": "SELECT count() FROM nyc_taxi.trips" }
```

**Response:**
```json
{
  "columns": ["count()"],
  "rows": [[1000660]],
  "elapsed_ms": 4.2,
  "row_count": 1
}
```

---

## Fases do pipeline

```
idle → downloading → inserting → ready
```

- **downloading** — streaming por chunks de 1 MB com barra de progresso em tempo real. Pulado automaticamente se o arquivo já existir com o tamanho correto (~78 MB).
- **inserting** — leitura linha a linha do gzip, insert em batches de 50.000 linhas. Progresso estimado pela posição no arquivo comprimido.
- **ready** — tabela disponível para queries.

---

## Queries de exemplo

```sql
-- Bairros com maior gorjeta média
SELECT pickup_ntaname, avg(tip_amount) AS avg_tip, count() AS trips
FROM nyc_taxi.trips
GROUP BY pickup_ntaname
ORDER BY avg_tip DESC
LIMIT 20;

-- Distribuição por número de passageiros
SELECT passenger_count, count() AS trips, avg(total_amount) AS avg_fare
FROM nyc_taxi.trips
GROUP BY passenger_count
ORDER BY passenger_count;

-- Top 10 corridas mais caras
SELECT trip_id, pickup_ntaname, dropoff_ntaname, total_amount, trip_distance
FROM nyc_taxi.trips
ORDER BY total_amount DESC
LIMIT 10;

-- Corridas por hora do dia
SELECT toHour(pickup_datetime) AS hour, count() AS trips
FROM nyc_taxi.trips
GROUP BY hour
ORDER BY hour;

-- Percentis de gorjeta por hora do dia
SELECT
    toHour(pickup_datetime) AS hour,
    quantile(0.5)(tip_amount)  AS p50,
    quantile(0.95)(tip_amount) AS p95,
    count() AS trips
FROM nyc_taxi.trips
GROUP BY hour
ORDER BY hour;
```

---

## Como executar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) com Docker Compose
- Python 3.11 ou superior

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd nyc-taxi-lab
```

### 2. Suba o ClickHouse

```bash
docker compose up -d
```

Aguarde alguns segundos até o container estar pronto. Você pode verificar com:

```bash
docker compose ps
```

### 3. Instale as dependências Python

```bash
pip install -r requirements.txt
```

> Recomendado: use um ambiente virtual (`python -m venv .venv && source .venv/bin/activate` no Linux/Mac ou `.venv\Scripts\activate` no Windows).

### 4. Inicie o servidor

```bash
uvicorn main:app --reload
```

### 5. Acesse no browser

```
http://localhost:8000
```

Clique em **INICIAR PIPELINE**. O download (~78 MB) começa imediatamente — se o arquivo já tiver sido baixado anteriormente, essa etapa é pulada. O insert de ~1M linhas leva cerca de 1 minuto.

### 6. Execute queries

Ao término do pipeline, o editor de queries aparece com uma query de exemplo já preenchida. Use **▶ EXECUTAR** ou `Ctrl+Enter` para rodar.

---

### Parar os serviços

```bash
# Para o servidor: Ctrl+C no terminal do uvicorn

# Para o ClickHouse (mantém os dados):
docker compose stop

# Para o ClickHouse e remove os dados:
docker compose down -v
```
