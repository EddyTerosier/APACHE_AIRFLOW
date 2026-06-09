import json
import logging
from datetime import datetime, timedelta

import requests

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

OPEN_METEO_URL    = "https://api.open-meteo.com/v1/forecast"
API_CURRENT_FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
POSTGRES_CONN_ID  = "weather_postgres"

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _get_cities() -> list[dict]:
    default = [
        {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
        {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
        {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
    ]
    return Variable.get("weather_cities", default_var=default, deserialize_json=True)


# ---------------------------------------------------------------------------
# extract_api_payload
# Appelle Open-Meteo pour chaque ville et pousse les payloads bruts en XCom.
# ---------------------------------------------------------------------------

def extract_api_payload(**context) -> None:
    cities = _get_cities()
    logger.info("Extraction démarrée — %d villes", len(cities))

    payloads = []
    for city in cities:
        params = {
            "latitude":  city["latitude"],
            "longitude": city["longitude"],
            "current":   API_CURRENT_FIELDS,
        }
        logger.info("Appel API Open-Meteo pour %s", city["name"])
        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        response.raise_for_status()

        payloads.append({
            "city":      city["name"],
            "latitude":  city["latitude"],
            "longitude": city["longitude"],
            "payload":   response.json(),
        })
        logger.info("Payload reçu pour %s", city["name"])

    logger.info("Extraction terminée — %d payloads", len(payloads))
    context["ti"].xcom_push(key="raw_payloads", value=payloads)


# ---------------------------------------------------------------------------
# store_raw_payload
# Insère les payloads bruts dans bronze.raw_weather_payloads.
# Pousse le run_id bronze en XCom pour la traçabilité aval.
# ---------------------------------------------------------------------------

def store_raw_payload(**context) -> None:
    payloads = context["ti"].xcom_pull(task_ids="extract_api_payload", key="raw_payloads")
    if not payloads:
        raise ValueError("Aucun payload disponible — extract_api_payload a échoué")

    run_id   = context["run_id"]
    hook     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ingested_at = datetime.utcnow()

    for entry in payloads:
        hook.run(
            """
            INSERT INTO bronze.raw_weather_payloads
                (run_id, city, latitude, longitude, payload_json, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                run_id,
                entry["city"],
                entry["latitude"],
                entry["longitude"],
                json.dumps(entry["payload"]),
                ingested_at,
            ),
        )
        logger.info("Payload bronze stocké pour %s (run_id=%s)", entry["city"], run_id)

    logger.info("Bronze — %d payloads insérés", len(payloads))
    context["ti"].xcom_push(key="bronze_run_id", value=run_id)


# ---------------------------------------------------------------------------
# transform_to_silver
# Lit les payloads bronze, sélectionne et normalise les champs utiles,
# insère dans silver.weather_observations.
# ---------------------------------------------------------------------------

def transform_to_silver(**context) -> None:
    payloads = context["ti"].xcom_pull(task_ids="extract_api_payload", key="raw_payloads")
    run_id   = context["ti"].xcom_pull(task_ids="store_raw_payload", key="bronze_run_id")

    if not payloads:
        raise ValueError("Aucun payload disponible pour la transformation silver")

    hook            = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    required_fields = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]
    records_ok      = 0

    for entry in payloads:
        city    = entry["city"]
        current = entry["payload"].get("current", {})

        missing = [f for f in required_fields if f not in current]
        if missing:
            raise ValueError("Champs manquants pour %s : %s" % (city, missing))

        hook.run(
            """
            INSERT INTO silver.weather_observations
                (city, observed_at, temperature_celsius, humidity_pct,
                 wind_speed_kmh, precipitation_mm, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                city,
                current.get("time", datetime.utcnow().isoformat()),
                current["temperature_2m"],
                current["relative_humidity_2m"],
                current["wind_speed_10m"],
                current["precipitation"],
                run_id,
            ),
        )
        records_ok += 1
        logger.info(
            "Silver OK — %s : %.1f°C | %d%% | %.1f km/h | %.1f mm",
            city,
            current["temperature_2m"],
            current["relative_humidity_2m"],
            current["wind_speed_10m"],
            current["precipitation"],
        )

    logger.info("Silver — %d enregistrements insérés", records_ok)
    context["ti"].xcom_push(key="records_silver", value=records_ok)


# ---------------------------------------------------------------------------
# load_gold_metrics
# Calcule les agrégats journaliers par ville et les charge dans
# gold.weather_daily_city (upsert sur city + observation_date).
# ---------------------------------------------------------------------------

def load_gold_metrics(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="store_raw_payload", key="bronze_run_id")
    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(
        """
        INSERT INTO gold.weather_daily_city
            (city, observation_date, avg_temperature, max_wind_speed,
             total_precipitation, run_id)
        SELECT
            city,
            DATE(observed_at)           AS observation_date,
            ROUND(AVG(temperature_celsius)::numeric, 2) AS avg_temperature,
            MAX(wind_speed_kmh)         AS max_wind_speed,
            SUM(precipitation_mm)       AS total_precipitation,
            %s                          AS run_id
        FROM silver.weather_observations
        WHERE DATE(observed_at) = CURRENT_DATE
        GROUP BY city, DATE(observed_at)
        ON CONFLICT (city, observation_date)
        DO UPDATE SET
            avg_temperature    = EXCLUDED.avg_temperature,
            max_wind_speed     = EXCLUDED.max_wind_speed,
            total_precipitation= EXCLUDED.total_precipitation,
            run_id             = EXCLUDED.run_id,
            updated_at         = NOW()
        """,
        parameters=(run_id,),
    )
    logger.info("Gold — agrégats journaliers chargés dans weather_daily_city (run_id=%s)", run_id)


# ---------------------------------------------------------------------------
# log_ingestion_run
# Écrit la trace d'exécution dans technical.ingestion_runs.
# trigger_rule="all_done" : s'exécute même si une tâche amont a échoué.
# ---------------------------------------------------------------------------

def log_ingestion_run(**context) -> None:
    ti      = context["ti"]
    dag_run = context["dag_run"]

    records_received = len(_get_cities())
    records_inserted = ti.xcom_pull(task_ids="transform_to_silver", key="records_silver") or 0

    all_states   = [t.state for t in dag_run.get_task_instances() if t.task_id != "log_ingestion_run"]
    run_status   = "success" if all(s == "success" for s in all_states) else "partial_failure"

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO technical.ingestion_runs
            (source, data_interval_start, data_interval_end,
             started_at, ended_at, status, records_received, records_inserted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            "open-meteo",
            context.get("data_interval_start"),
            context.get("data_interval_end"),
            dag_run.start_date,
            datetime.utcnow(),
            run_status,
            records_received,
            records_inserted,
        ),
    )
    logger.info(
        "Run tracé — status=%s | reçues=%d | insérées=%d",
        run_status, records_received, records_inserted,
    )


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="weather_pipeline",
    description="Open-Meteo → bronze → silver → gold + traçabilité",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meteo", "tp2b"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_api_payload",
        python_callable=extract_api_payload,
    )

    task_store_bronze = PythonOperator(
        task_id="store_raw_payload",
        python_callable=store_raw_payload,
    )

    task_transform_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    task_load_gold = PythonOperator(
        task_id="load_gold_metrics",
        python_callable=load_gold_metrics,
    )

    task_log = PythonOperator(
        task_id="log_ingestion_run",
        python_callable=log_ingestion_run,
        trigger_rule="all_done",
    )

    task_extract >> task_store_bronze >> task_transform_silver >> task_load_gold >> task_log
