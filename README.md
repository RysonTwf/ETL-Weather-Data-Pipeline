# Weather ETL Pipeline

An automated ETL pipeline that pulls daily weather data from [Open-Meteo](https://open-meteo.com/) (free, no API key), transforms it with Python and dbt, loads it into PostgreSQL, and schedules runs with Apache Airflow — all containerised with Docker.

---

## Stack

| Layer         | Technology                  |
|---------------|-----------------------------|
| Extract       | Python + Open-Meteo API     |
| Transform     | Pandas + dbt                |
| Load          | SQLAlchemy + psycopg2       |
| Database      | PostgreSQL 15               |
| Orchestrate   | Apache Airflow 2.9          |
| Runtime       | Docker + Docker Compose     |
| Testing       | pytest                      |

---

## Project Structure

```
weather_project/
├── dags/
│   └── weather_dag.py            # Airflow DAG (5 tasks)
├── pipeline/
│   ├── extract.py                # Fetch from Open-Meteo API
│   ├── transform.py              # Parse JSON → DataFrames
│   └── load.py                   # Upsert into PostgreSQL
├── dbt/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # dbt connection config
│   └── models/
│       ├── sources.yml           # Declares raw_weather as a source
│       └── daily_summary.sql     # dbt model: aggregates raw_weather
├── sql/
│   └── create_tables.sql         # Schema: raw_weather table
├── tests/
│   └── test_transform.py         # pytest unit tests (13 tests)
├── Dockerfile.airflow            # Airflow image + dbt-postgres
├── docker-compose.yml            # All services
├── conftest.py                   # pytest path setup
├── requirements.txt
└── README.md
```

---

## Architecture

```
Open-Meteo API
      │
      ▼
  [extract]          Python — fetches last 7 days of weather data
      │
      ▼
  [transform]        Python/Pandas — parses JSON into a clean DataFrame
      │
      ▼
  [load_raw]         SQLAlchemy — upserts into raw_weather
                                  (ON CONFLICT DO NOTHING prevents duplicates)
      │
      ▼
  [dbt_run]          dbt — builds daily_summary from raw_weather
      │
      ▼
  [quality_check]    Asserts both tables have > 0 rows; fails the DAG if not
```

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- (Optional) DBeaver for inspecting the database

### 1. Start all services

```bash
docker-compose up -d --build
```

This will:
- Build a custom Airflow image with `dbt-postgres` installed
- Start `weather_db` (PostgreSQL on port 5432) and auto-create `raw_weather`
- Start `airflow_db` (Airflow metadata database)
- Run `airflow-init` to migrate the DB and create the admin user
- Start `airflow-webserver` (port 8080) and `airflow-scheduler`

Wait ~60 seconds for all services to be healthy.

### 2. Open the Airflow UI

Navigate to [http://localhost:8080](http://localhost:8080)

- **Username:** `airflow`
- **Password:** `airflow`

### 3. Run the DAG

1. Find `weather_etl` in the DAGs list
2. Toggle it **On**
3. Click **Trigger DAG** to run immediately
4. Watch all 5 tasks go green: `extract → transform → load_raw → dbt_run → quality_check`

### 4. Verify data in DBeaver

Connect DBeaver to:
- **Host:** `localhost` | **Port:** `5432`
- **Database:** `weather` | **User:** `admin` | **Password:** `password`

```sql
SELECT * FROM raw_weather ORDER BY date DESC;
SELECT * FROM daily_summary ORDER BY date DESC;
```

---

## DAG Tasks

| Task            | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `extract`       | Calls Open-Meteo API, pushes raw JSON to XCom                     |
| `transform`     | Parses JSON into a DataFrame, pushes to XCom                      |
| `load_raw`      | Upserts rows into `raw_weather` — skips duplicates automatically   |
| `dbt_run`       | Runs dbt to build `daily_summary` from `raw_weather`              |
| `quality_check` | Asserts both tables have > 0 rows; fails the run if not           |

The DAG runs on a `@daily` schedule and is idempotent — re-running it never creates duplicate rows.

---

## dbt

dbt manages the `daily_summary` table as a model, keeping SQL transformations version-controlled and testable separately from the load logic.

```
dbt/
└── models/
    ├── sources.yml          # Declares raw_weather as the upstream source
    └── daily_summary.sql    # SELECT + aggregation logic
```

To run dbt manually (inside the Airflow container):
```bash
docker exec -it airflow_scheduler \
  dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

---

## Running Tests

```bash
pip install pandas pytest
pytest tests/ -v
```

Tests cover the `transform` step using a mock API response — no network or database connection needed.

---

## Running Scripts Locally (without Docker)

Requires PostgreSQL running on `localhost:5432` with the schema applied.

```bash
pip install -r requirements.txt

python pipeline/extract.py    # prints raw JSON
python pipeline/transform.py  # prints DataFrames
python pipeline/load.py       # upserts into DB
```

---

## Stopping Services

```bash
docker-compose down       # stop containers, keep data volumes
docker-compose down -v    # stop containers and delete all data
```

---

## Resume Bullet Point

> Built an automated ETL pipeline using Python, dbt, and Apache Airflow to ingest, transform, and load daily weather data into PostgreSQL on a scheduled basis; implemented idempotent upserts to prevent duplicate rows and pytest unit tests to validate transformation logic.
