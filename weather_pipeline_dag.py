"""
DAG : weather_pipeline
======================
TP 2A — Préparer une ingestion API météo

Workflow :
    extract_weather  →  transform_weather  →  load_weather

Séparation des responsabilités :
    extract_weather   : appel API uniquement — ne transforme pas
    transform_weather : sélection et normalisation des champs — n'appelle pas l'API
    load_weather      : écriture en base — ne fait ni appel ni transformation

Passage d'information entre tâches :
    extract   → XCom clé "raw_weather"       → transform
    transform → XCom clé "clean_weather"     → load

Les XComs transportent de petits objets (3 villes × quelques champs).
"""

import logging
from datetime import datetime, timedelta

import requests

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CITIES = [
    {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]

# Champs récupérés depuis l'API Open-Meteo
# Référence : https://open-meteo.com/en/docs
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
API_CURRENT_FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"

# Champs retenus pour la table cible (justification dans le README)
REQUIRED_FIELDS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Fonctions métier
# ---------------------------------------------------------------------------

def extract_weather(**context) -> None:
    """
    Étape 1 — Extraction : appel API Open-Meteo pour chaque ville.

    Responsabilité unique : récupérer la matière première brute.
    Cette fonction ne sélectionne pas les champs et ne transforme pas.
    """
    logger.info("Démarrage de l'extraction — %d villes à interroger", len(CITIES))

    raw_results = []

    for city in CITIES:
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "current": API_CURRENT_FIELDS,
        }

        logger.info(
            "Appel API Open-Meteo pour %s (lat=%.4f, lon=%.4f)",
            city["name"], city["latitude"], city["longitude"],
        )

        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)

        # Lève HTTPError si statut 4xx ou 5xx
        response.raise_for_status()

        raw_current = response.json().get("current", {})

        logger.info(
            "Réponse brute reçue pour %s : %s",
            city["name"], raw_current,
        )

        raw_results.append({
            "city": city["name"],
            "raw": raw_current,
        })

    logger.info("Extraction terminée — %d villes récupérées", len(raw_results))

    # Transmission à la tâche suivante via XCom
    # Volume : 3 villes × ~5 champs = objet léger, usage XCom justifié
    context["ti"].xcom_push(key="raw_weather", value=raw_results)


def transform_weather(**context) -> None:
    """
    Étape 2 — Transformation : sélection et normalisation des champs utiles.

    Responsabilité unique : préparer la donnée pour la table cible.
    Cette fonction ne contacte pas l'API et ne charge pas en base.
    """
    logger.info("Démarrage de la transformation")

    # Récupération des données brutes produites par extract_weather
    raw_results = context["ti"].xcom_pull(
        task_ids="extract_weather",
        key="raw_weather",
    )

    if not raw_results:
        raise ValueError("Aucune donnée brute disponible — la tâche extract_weather a peut-être échoué")

    clean_results = []

    for entry in raw_results:
        city = entry["city"]
        raw = entry["raw"]

        # Vérification que les champs attendus sont présents dans la réponse
        missing = [f for f in REQUIRED_FIELDS if f not in raw]
        if missing:
            raise ValueError("Champs manquants pour %s : %s" % (city, missing))

        # Sélection et renommage explicite des champs pour la table cible
        # On renomme pour que le nom de colonne soit clair et indépendant
        # des conventions de l'API source
        clean_record = {
            "city":               city,
            "temperature_celsius": raw["temperature_2m"],
            "humidity_pct":        raw["relative_humidity_2m"],
            "wind_speed_kmh":      raw["wind_speed_10m"],
            "precipitation_mm":    raw["precipitation"],
            "fetched_at":          raw.get("time", datetime.utcnow().isoformat()),
        }

        logger.info(
            "Transformation OK — %s : %.1f°C | %d%% | %.1f km/h | %.1f mm",
            city,
            clean_record["temperature_celsius"],
            clean_record["humidity_pct"],
            clean_record["wind_speed_kmh"],
            clean_record["precipitation_mm"],
        )

        clean_results.append(clean_record)

    logger.info("Transformation terminée — %d enregistrements prêts", len(clean_results))

    # Transmission à la tâche suivante via XCom
    context["ti"].xcom_push(key="clean_weather", value=clean_results)


def load_weather(**context) -> None:
    """
    Étape 3 — Chargement : écriture des données nettoyées en base.

    Responsabilité unique : écriture uniquement.
    Cette fonction ne contacte pas l'API et ne fait pas de transformation.
    """
    logger.info("Démarrage du chargement en base de données")

    # Récupération des données transformées produites par transform_weather
    clean_results = context["ti"].xcom_pull(
        task_ids="transform_weather",
        key="clean_weather",
    )

    if not clean_results:
        raise ValueError("Aucune donnée propre disponible — la tâche transform_weather a peut-être échoué")

    for record in clean_results:
        # Simulation de l'INSERT
        logger.info(
            "[SIMULATION INSERT] weather_data — "
            "city=%s | temp=%.1f°C | humidity=%d%% | wind=%.1f km/h | precip=%.1f mm | at=%s",
            record["city"],
            record["temperature_celsius"],
            record["humidity_pct"],
            record["wind_speed_kmh"],
            record["precipitation_mm"],
            record["fetched_at"],
        )

    logger.info(
        "Chargement terminé — %d lignes insérées en base", len(clean_results)
    )


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="weather_pipeline",
    description="Ingestion quotidienne météo Open-Meteo — TP 2A",
    schedule="0 6 * * *",       # tous les jours à 06h00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meteo", "tp2a"],
) as dag:

    # ── Tâche 1 : Extraction ────────────────────────────────────────────────
    task_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    # ── Tâche 2 : Transformation ────────────────────────────────────────────
    task_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    # ── Tâche 3 : Chargement ────────────────────────────────────────────────
    task_load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    # ── Dépendances ─────────────────────────────────────────────────────────
    # extract_weather → transform_weather → load_weather
    task_extract >> task_transform >> task_load