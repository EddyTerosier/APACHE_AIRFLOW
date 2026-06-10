import sys
import os
import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Rend les modules du dossier include/ importables depuis le DAG
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))

import extract
import transform
import quality
import db

logger = logging.getLogger(__name__)

SOURCE_NAME = "open-meteo"

# Robustesse : retries pour les incidents temporaires, timeout pour éviter
# qu'une tâche reste bloquée. Appliqué par défaut, ajusté par tâche si besoin.
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
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


def extract_api_payload(**context) -> None:
    """Récupère les payloads bruts depuis Open-Meteo et les archive en bronze.

    L'écriture bronze est l'archivage de la preuve source : c'est sa raison
    d'être. Elle est idempotente par run_id.
    """
    cities = _get_cities()
    payloads = extract.fetch_all_cities(cities)

    run_id = context["run_id"]
    db.insert_bronze(run_id, payloads)

    context["ti"].xcom_push(key="raw_payloads", value=payloads)


def transform_payload(**context) -> None:
    """Transforme les payloads bruts en observations propres, en mémoire.

    Aucune écriture en base : les observations sont transmises via XCom.
    Le chargement silver n'aura lieu qu'après validation qualité.
    """
    payloads = context["ti"].xcom_pull(task_ids="extract_api_payload", key="raw_payloads")
    if not payloads:
        raise ValueError("Aucun payload disponible — extract_api_payload a échoué")

    observations = transform.transform_all(payloads)
    context["ti"].xcom_push(key="observations", value=observations)


def run_quality_checks(**context) -> None:
    """Applique les contrôles qualité sur les observations transformées.

    S'exécute AVANT toute écriture silver/gold. Trace le verdict dans
    technical.data_quality_results. Pousse le résultat en XCom pour le branchement.
    """
    observations = context["ti"].xcom_pull(task_ids="transform_payload", key="observations")
    report = quality.run_quality_checks(observations or [])

    run_id = context["run_id"]
    status = "passed" if report["passed"] else "failed"
    db.insert_quality_result(run_id, status, report["anomalies"], report["checked"])

    context["ti"].xcom_push(key="quality_passed", value=report["passed"])


def decide_branch(**context) -> str:
    """Branchement conditionnel : oriente vers le chargement ou l'alerte.

    Règle métier : si la qualité est validée, on charge les données (silver puis gold).
    Sinon, on bascule sur le chemin d'alerte et AUCUNE donnée n'est chargée.
    """
    quality_passed = context["ti"].xcom_pull(task_ids="run_quality_checks", key="quality_passed")

    if quality_passed:
        logger.info("Qualité validée — chemin nominal (chargement silver puis gold)")
        return "load_silver"

    logger.warning("Qualité invalide — chemin d'alerte (aucun chargement)")
    return "raise_quality_alert"


def load_silver(**context) -> None:
    """Chemin nominal : charge les observations validées dans silver (upsert idempotent).

    N'est atteinte qu'après validation qualité réussie.
    """
    observations = context["ti"].xcom_pull(task_ids="transform_payload", key="observations")
    if not observations:
        raise ValueError("Aucune observation à charger — transform_payload a échoué")

    run_id = context["run_id"]
    inserted = db.insert_silver(run_id, observations)
    context["ti"].xcom_push(key="records_inserted", value=inserted)


def load_gold_metrics(**context) -> None:
    """Chemin nominal : charge les agrégats journaliers dans gold (upsert idempotent)."""
    run_id = context["run_id"]
    db.upsert_gold(run_id)


def raise_quality_alert(**context) -> None:
    """Chemin d'alerte : journalise l'anomalie. Aucune donnée chargée (ni silver ni gold)."""
    run_id = context["run_id"]
    logger.error(
        "ALERTE QUALITÉ — run_id=%s : chargement bloqué, données non exploitables. "
        "Voir technical.data_quality_results.",
        run_id,
    )


def log_ingestion_run(**context) -> None:
    """Trace l'exécution globale dans technical.ingestion_runs.

    trigger_rule=all_done : s'exécute quel que soit le chemin emprunté
    (nominal ou alerte) et même en cas d'échec amont.
    """
    ti = context["ti"]
    dag_run = context["dag_run"]

    records_received = len(_get_cities())
    records_inserted = ti.xcom_pull(task_ids="load_silver", key="records_inserted") or 0
    quality_passed = ti.xcom_pull(task_ids="run_quality_checks", key="quality_passed")

    if quality_passed is False:
        status = "rejected_quality"
    elif all(t.state == "success" for t in dag_run.get_task_instances()
             if t.task_id not in ("log_ingestion_run", "raise_quality_alert")):
        status = "success"
    else:
        status = "partial_failure"

    db.insert_ingestion_run(
        run_id=context["run_id"],
        source=SOURCE_NAME,
        data_interval_start=context.get("data_interval_start"),
        data_interval_end=context.get("data_interval_end"),
        started_at=dag_run.start_date,
        ended_at=datetime.utcnow(),
        status=status,
        records_received=records_received,
        records_inserted=records_inserted,
    )


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline météo industrialisé Open-Meteo — TP5",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meteo", "tp5"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_api_payload",
        python_callable=extract_api_payload,
        # Timeout court : un appel API ne doit jamais traîner
        execution_timeout=timedelta(minutes=2),
    )

    task_transform = PythonOperator(
        task_id="transform_payload",
        python_callable=transform_payload,
    )

    task_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        # Erreur de qualité = échec structurel : pas de retry inutile
        retries=0,
    )

    task_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    task_load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
    )

    task_load_gold = PythonOperator(
        task_id="load_gold_metrics",
        python_callable=load_gold_metrics,
    )

    task_alert = PythonOperator(
        task_id="raise_quality_alert",
        python_callable=raise_quality_alert,
        retries=0,
    )

    # Point de convergence des deux chemins avant la traçabilité finale
    task_join = EmptyOperator(
        task_id="join_paths",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    task_log = PythonOperator(
        task_id="log_ingestion_run",
        python_callable=log_ingestion_run,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    # Chemin commun : extraction → archivage bronze → transformation → qualité → décision
    task_extract >> task_transform >> task_quality >> task_branch

    # Chemin nominal : chargement silver puis gold (après validation)
    task_branch >> task_load_silver >> task_load_gold >> task_join

    # Chemin d'alerte : aucune écriture de données
    task_branch >> task_alert >> task_join

    task_join >> task_log