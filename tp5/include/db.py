"""Accès à la base PostgreSQL via PostgresHook et chargement des scripts SQL."""

import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "weather_postgres"

# Dossier des scripts SQL, monté à côté des modules include/
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def get_hook() -> PostgresHook:
    """Retourne un PostgresHook configuré sur la connexion weather_postgres."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def load_sql(filename: str) -> str:
    """Charge le contenu d'un script SQL depuis le dossier sql/."""
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def insert_bronze(run_id: str, payloads: list[dict]) -> int:
    """Insère les payloads bruts dans bronze.raw_weather_payloads.

    L'insertion bronze est idempotente par run_id : on supprime d'abord
    les lignes du run avant de réinsérer, ce qui rend la tâche rejouable.
    """
    import json

    hook = get_hook()

    # Idempotence : on efface ce qui appartient à ce run avant de réinsérer
    hook.run(
        "DELETE FROM bronze.raw_weather_payloads WHERE run_id = %s",
        parameters=(run_id,),
    )

    for entry in payloads:
        hook.run(
            load_sql("insert_bronze.sql"),
            parameters=(
                run_id,
                entry["city"],
                entry["latitude"],
                entry["longitude"],
                json.dumps(entry["payload"]),
            ),
        )

    logger.info("Bronze — %d payloads insérés (run_id=%s)", len(payloads), run_id)
    return len(payloads)


def insert_silver(run_id: str, observations: list[dict]) -> int:
    """Insère les observations propres dans silver.weather_observations.

    Idempotent par run_id : suppression préalable des lignes du run.
    """
    hook = get_hook()

    hook.run(
        "DELETE FROM silver.weather_observations WHERE run_id = %s",
        parameters=(run_id,),
    )

    insert_stmt = load_sql("insert_silver.sql")
    for obs in observations:
        hook.run(
            insert_stmt,
            parameters=(
                obs["city"],
                obs["observed_at"],
                obs["temperature_celsius"],
                obs["humidity_pct"],
                obs["wind_speed_kmh"],
                obs["precipitation_mm"],
                run_id,
            ),
        )

    logger.info("Silver — %d observations insérées (run_id=%s)", len(observations), run_id)
    return len(observations)


def upsert_gold(run_id: str) -> None:
    """Recalcule et upserte les agrégats journaliers dans gold.weather_daily_city.

    Idempotent grâce à ON CONFLICT (city, observation_date).
    """
    hook = get_hook()
    hook.run(load_sql("upsert_gold.sql"), parameters=(run_id,))
    logger.info("Gold — agrégats journaliers upsertés (run_id=%s)", run_id)


def insert_quality_result(run_id: str, status: str, anomalies: list[str], checked: int) -> None:
    """Enregistre le résultat du contrôle qualité dans technical.data_quality_results."""
    hook = get_hook()
    detail = "; ".join(anomalies) if anomalies else "Aucune anomalie"
    hook.run(
        load_sql("insert_quality_result.sql"),
        parameters=(run_id, status, checked, len(anomalies), detail),
    )
    logger.info("Qualité tracée — run_id=%s | status=%s | anomalies=%d", run_id, status, len(anomalies))


def insert_ingestion_run(
    run_id: str,
    source: str,
    data_interval_start,
    data_interval_end,
    started_at,
    ended_at,
    status: str,
    records_received: int,
    records_inserted: int,
) -> None:
    """Enregistre la trace globale de l'exécution dans technical.ingestion_runs."""
    hook = get_hook()
    hook.run(
        load_sql("insert_ingestion_run.sql"),
        parameters=(
            run_id, source, data_interval_start, data_interval_end,
            started_at, ended_at, status, records_received, records_inserted,
        ),
    )
    logger.info("Run tracé — run_id=%s | status=%s", run_id, status)