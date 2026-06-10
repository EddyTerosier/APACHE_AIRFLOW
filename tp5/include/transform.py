"""Logique de transformation des payloads bruts en observations propres."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Champs attendus dans le bloc "current" de la réponse Open-Meteo
REQUIRED_API_FIELDS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]


def transform_payload(entry: dict) -> dict:
    """Transforme un payload brut d'une ville en observation normalisée.

    Sélectionne les champs utiles et les renomme pour la table silver.
    Lève KeyError si un champ attendu est absent du payload.
    """
    city = entry["city"]
    current = entry["payload"].get("current", {})

    observation = {
        "city": city,
        "observed_at": current.get("time", datetime.utcnow().isoformat()),
        "temperature_celsius": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
    }

    logger.info(
        "Transformation OK — %s : %.1f°C | %d%% | %.1f km/h | %.1f mm",
        city,
        observation["temperature_celsius"],
        observation["humidity_pct"],
        observation["wind_speed_kmh"],
        observation["precipitation_mm"],
    )
    return observation


def transform_all(payloads: list[dict]) -> list[dict]:
    """Transforme tous les payloads bruts en liste d'observations propres.

    Transformation en mémoire uniquement : aucune écriture en base.
    Le chargement silver intervient après la validation qualité.
    """
    observations = [transform_payload(entry) for entry in payloads]
    logger.info("Transformation terminée — %d observations", len(observations))
    return observations