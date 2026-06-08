import logging
from datetime import datetime, timedelta

import requests

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration des villes
# Coordonnées GPS nécessaires pour l'API Open-Meteo
# ---------------------------------------------------------------------------

CITIES = [
    {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Fonctions métier — une fonction = une responsabilité
# ---------------------------------------------------------------------------

def fetch_weather() -> None:
    """
    Étape 1 : Récupère les données météo brutes depuis l'API Open-Meteo.

    Rôle : appel réseau uniquement. Cette fonction ne transforme pas
    et ne valide pas les données.

    API utilisée : https://open-meteo.com (gratuite, sans clé)
    Champs récupérés :
      - temperature_2m        : température à 2m du sol (°C)
      - relative_humidity_2m  : humidité relative à 2m (%)
      - wind_speed_10m        : vitesse du vent à 10m (km/h)
    """
    logger.info("Démarrage de la récupération météo — %d villes", len(CITIES))

    for city in CITIES:
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        }

        logger.info("Appel API pour %s (lat=%.4f, lon=%.4f)", city["name"], city["latitude"], city["longitude"])

        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)

        # Lève une exception si le statut HTTP est une erreur (4xx, 5xx)
        response.raise_for_status()

        data = response.json()
        current = data.get("current", {})

        logger.info(
            "Données reçues pour %s : température=%.1f°C  humidité=%d%%  vent=%.1f km/h",
            city["name"],
            current.get("temperature_2m", float("nan")),
            current.get("relative_humidity_2m", -1),
            current.get("wind_speed_10m", float("nan")),
        )

    logger.info("Récupération terminée pour %d villes", len(CITIES))


def validate_weather() -> None:
    """
    Étape 2 : Vérifie que les données récupérées sont complètes et cohérentes.

    Rôle : contrôle qualité uniquement. Cette fonction ne charge pas en base
    et ne contacte pas l'API.

    Règles vérifiées :
      - Les champs obligatoires sont présents dans la réponse API.
      - La température est dans une plage réaliste (−50 °C à 60 °C).
      - L'humidité est entre 0 % et 100 %.
      - La vitesse du vent est positive.
    """
    logger.info("Démarrage de la validation des données météo")

    required_fields = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]

    for city in CITIES:
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        }

        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        response.raise_for_status()

        current = response.json().get("current", {})

        # Vérification des champs obligatoires
        missing = [f for f in required_fields if f not in current]
        if missing:
            raise ValueError("Champs manquants pour %s : %s" % (city["name"], missing))

        temperature = current["temperature_2m"]
        humidity    = current["relative_humidity_2m"]
        wind_speed  = current["wind_speed_10m"]

        # Vérification des plages métier
        if not (-50 <= temperature <= 60):
            raise ValueError(
                "Température hors plage pour %s : %.1f°C" % (city["name"], temperature)
            )

        if not (0 <= humidity <= 100):
            raise ValueError(
                "Humidité hors plage pour %s : %d%%" % (city["name"], humidity)
            )

        if wind_speed < 0:
            raise ValueError(
                "Vitesse du vent négative pour %s : %.1f km/h" % (city["name"], wind_speed)
            )

        logger.info(
            "Validation OK — %s : %.1f°C | %d%% | %.1f km/h",
            city["name"], temperature, humidity, wind_speed,
        )

    logger.info("Validation terminée : %d enregistrements valides", len(CITIES))


def load_weather() -> None:
    """
    Étape 3 : Charge les données validées en base de données.

    Rôle : écriture uniquement. Cette fonction ne récupère pas de données
    depuis l'API et ne fait pas de validation métier.

    Schéma cible attendu :
      CREATE TABLE weather_data (
          city        TEXT,
          temperature FLOAT,
          humidity    INT,
          wind_speed  FLOAT,
          fetched_at  TIMESTAMP
      );
    """
    logger.info("Démarrage du chargement en base de données")

    for city in CITIES:
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        }

        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        response.raise_for_status()

        current = response.json().get("current", {})

        # Simulation de l'écriture — en production : INSERT réel
        logger.info(
            "[SIMULATION INSERT] weather_data — city=%s  temp=%.1f  humidity=%d  wind=%.1f  at=%s",
            city["name"],
            current.get("temperature_2m"),
            current.get("relative_humidity_2m"),
            current.get("wind_speed_10m"),
            datetime.utcnow().isoformat(),
        )

    logger.info("Chargement terminé : %d lignes insérées en base", len(CITIES))


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="weather_pipeline",
    description="Récupération quotidienne de la météo via Open-Meteo — TP 2",
    schedule="0 6 * * *",       # tous les jours à 06h00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,               # ne pas rejouer les runs passés manquants
    default_args=DEFAULT_ARGS,
    tags=["meteo", "tp2"],
) as dag:

    # ── Tâche 1 : Récupération ──────────────────────────────────────────────
    task_fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    # ── Tâche 2 : Validation ────────────────────────────────────────────────
    task_validate = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather,
    )

    # ── Tâche 3 : Chargement ────────────────────────────────────────────────
    task_load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    # ── Dépendances (ordre d'exécution) ─────────────────────────────────────
    # fetch_weather → validate_weather → load_weather
    task_fetch >> task_validate >> task_load
