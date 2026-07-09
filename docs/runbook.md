# Runbook

This document contains operational notes for running this stack locally, helpful for trying it out or developing over it.

This runbook assumes Docker Desktop on Windows with the WSL2 backend, however it can be adapted to any OS and Docker environment.

I developed this project using
- Windows 11 25H2
- Docker Desktop 4.45.0
- WSL 2.7.3.0
- Python 3.11.5
- pip 26.1.2
- Terraform 1.15.7

on an AMD x64 CPU with 8 cores (16 threads) and 16 GB RAM.

**NOTE:** this pipeline requires to read a big amount of data to work properly, otherwise the data analysis won't make sense. You must run this stack for at least a few hours before trying it out (a few days would be better) to collect enough data.

## Prerequisites

- [Python 3.11](https://www.python.org/downloads/release/python-3115/) or compatible versions;
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) with [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) backend;
- A [Twitch developer](https://dev.twitch.tv/) app (save client ID and secret);
- [Terraform](https://developer.hashicorp.com/terraform/install), if you want MinIO buckets managed declaratively instead of manual creation inside MinIO console.
- A `.env` file in the repo root with at least:

```dotenv
SPARK_APP_CORES=1       # suggested
SPARK_APP_MEMORY=1g     # suggested
SPARK_NUM_PARTITIONS=2  # suggested

TOPIC_STREAMS=twitch-streams  # suggested
TOPIC_GAMES=twitch-games      # suggested

TWITCH_CLIENT_ID=
TWITCH_CLIENT_SECRET=

AIRFLOW_UID=
AIRFLOW_SECRET_KEY=
AIRFLOW_WEB_USER=
AIRFLOW_WEB_FIRSTNAME=
AIRFLOW_WEB_LASTNAME=
AIRFLOW_WEB_EMAIL=
AIRFLOW_WEB_PASSWORD=

MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=

POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=twitch  # necessary as hardcoded in ClickHouse SQL
```

All users, passwords and email can be invented.

`SPARK_APP_CORES` and `SPARK_APP_MEMORY` specify the maximum amount of cores each Spark app can have. Considering that at least 5 apps are always running at the same time (and cores and memory are also necessary for your Docker instance for Airflow, Kafka, MinIO, Clickhouse containers), I suggest to keep those values low when running locally (but you can double them if you have more RAM or CPU than me).

`SPARK_NUM_PARTITIONS` is only read by the Airflow-triggered batch jobs (nightly optimization, gold hourly job). The long-running streaming jobs started by `spark_submit_jobs.sh` don't get it passed in; if shuffle partitions need tuning there, it has to be set inside the job itself (see the WSL2 section below, `transitions.py` already hardcodes `spark.sql.shuffle.partitions=8` for this reason).

## Cold start (first run)

We assume that commands are run in the project's root folder.

1. **Build the images.** Run `make build`. This can easily take up to 10 minutes. The `base` stage in the Dockerfile downloads ten JARs (Kafka connector, Delta, Hadoop-AWS, ClickHouse connector, etc.) from Maven Central, and both the `spark` and `airflow` images layer are built on top of that. Worth doing on a decent connection.

2. **Create the MinIO buckets.** `terraform/main.tf` creates three MinIO buckets.  
MinIO has to already be up for Terraform to talk to it, so run:
   ```powershell
   docker compose up -d minio
   make tf-init
   make tf-apply
   ```

3. **Start the entire pipeline.** Run `make up`. Postgres will start, then Airflow will be initialized (with your credentials) and started; Kafka will be started as well and topics will be automatically created. ClickHouse will run and the `clickhouse/init/*.sql` scripts will be executed to initialize the database schema. Lastly, Spark will start the bronze and silver streaming jobs.

4. **Install the local dependencies.** Run in Powershell:
   ```powershell
   python3 -m venv venv
   ./.venv/Scripts/Activate.ps1
   pip install -r ./requirements.txt
   ```
  
    This creates an isolated Python virtual environment and installs local dependencies.

5. **Run the dashboard.** Run `make dashboard`. The dashboard is not in Docker compose; it is a local Streamlit app that reads Delta tables directly off MinIO over `localhost:9000`.

## What runs and when

Apart from the obvious containers which stay running, that include **Spark master**, **Spark worker**, **Kafka**, **MinIO**, **Airflow** (and all its subprocesses), **PostgreSQL** and **ClickHouse**, there are various applications / jobs running on top of Spark or Airflow with different schedules:

- **Spark jobs**: there are are 5 Spark Structured Streaming applications always running. They are started by the init container `spark-stream-init`, which in turn runs the shell script `spark_submit_jobs.sh` to start them safely. 
  - bronze app (that consumes messages on Kafka topics constantly, receiving Twitch API data);
  - silver app for streams table;
  - silver app for tags table;
  - silver app for transitions table;
  - silver app for tags table.

  All silver apps trigger every 60 seconds to read bronze raw data tables from Delta Lake.
  
  If running for the first time, the silver jobs will instantly crash and automatically restart about a minute later than the bronze job (to wait for its first data ingestion).

  If the bronze job crashes, the bash script terminates and all the containers are restarted immediately. If a silver job crashes, it gets restarted after 60 seconds.  
  This is because the bronze job should always be online (to avoid bottlenecking the Kafka topic), while the silver are disposable and can be run anytime as their input data is stored on MinIO.

- **Airflow DAGs**: Three DAGs are run periodically:
  - **API ingestion** (`twitch_api_ingestion_dag`): once a minute, polls the Twitch API and pushes the JSON data to Kafka.
  - **Delta Lake optimization** (`nightly_maintenance_dag`): to avoid the small files problem, each night the bronze and silver Delta Lakes are optimized by merging small files into bigger ones;
  - **Gold layer transformation** (`gold_hourly_transformation_dag`): every hour the gold transformation Spark job takes silver data and aggregates it into the ClickHouse data warehouse (thus populating the gold layer).

  In particular, the API ingestion is run as a simple Python script inside the Airflow container, while the other two DAGs are Spark jobs submitted via `SparkSubmitOperator` to the Spark master which runs them (even though the Airflow container acts as a driver).

  All those jobs exit when they have finished (they don't stay continuously running).

## Service endpoints

| Service | URL | Notes |
|---|---|---|
| Airflow web UI | [http://localhost:8080](http://localhost:8080) | |
| MinIO web console | [http://localhost:9001](http://localhost:9001) | API on `9000`; internal is `minio:9000` |
| Spark master web UI | [http://localhost:8081](http://localhost:8081) | |
| Spark worker web UI | [http://localhost:8082](http://localhost:8082) | |
| ClickHouse HTTP | [http://localhost:8123](http://localhost:8123) | |
| ClickHouse native | localhost:9005 | |
| Kafka | localhost:9092 | External listener; internal is `kafka:29092` |

Airflow and MinIO web UIs will ask for authentication; the credentials are in your `.env` file.

## Troubleshooting

**1. Docker Engine goes unresponsive / every `docker` command returns a 500.**
I dealt with this problem very often during development. On Windows this is almost always the WSL2 VM running out of memory. `applyInPandasWithState` (the transitions job) and a shuffle partition count that's too high for the box are the usual suspects, as both keep a lot of state in memory per partition.  
After some time, Docker unfreezes itself, so first try to wait a few minutes (up to 10 minutes I would say).
If it happens repeatedly: lower `SPARK_NUM_PARTITIONS` or the transitions job's own `spark.sql.shuffle.partitions` setting, lower `SPARK_APP_CORES` and `SPARK_APP_MEMORY` if set too high, and consider increasing WSL2's memory in `.wslconfig` if possible.

**2. `STREAMING_STATEFUL_OPERATOR_NOT_MATCH_IN_STATE_METADATA`.** The
checkpoint at a given `s3a://.../checkpoints/<job>/` path was written by a
different version of that job's logic (schema change, different grouping
key, etc.), and Spark refuses to resume from a state that no longer matches.  
The fix is to delete just that job's checkpoint folder in MinIO and let it
rebuild from bronze; don't wipe the whole `checkpoints/` folder if the other three silver jobs' checkpoints are unrelated and still work.