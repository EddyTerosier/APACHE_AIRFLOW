"""Contrôles qualité appliqués aux observations avant chargement final.

Implémente les cinq familles de contrôles vues en cours :
complétude, présence des champs, cohérence, unicité, fraîcheur.
"""

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Champs obligatoires pour qu'une observation soit exploitable
REQUIRED_FIELDS = [
    "city", "observed_at", "temperature_celsius",
    "humidity_pct", "wind_speed_kmh", "precipitation_mm",
]

# Bornes de cohérence métier
TEMPERATURE_MIN_C = -50.0
TEMPERATURE_MAX_C = 60.0
HUMIDITY_MIN_PCT = 0
HUMIDITY_MAX_PCT = 100

# Seuil de fraîcheur : une mesure de plus de 24h est considérée périmée
FRESHNESS_MAX_AGE_HOURS = 24


def run_quality_checks(observations: list[dict]) -> dict:
    """Applique les contrôles qualité et retourne un rapport.
    Ne lève pas d'exception : la décision de bloquer revient au DAG (branchement).
    """
    anomalies: list[str] = []

    if not observations:
        anomalies.append("Aucune observation reçue (0 ligne)")
        return {"passed": False, "checked": 0, "anomalies": anomalies}

    # Contrôle d'unicité : une seule ligne par (ville, date d'observation)
    seen_keys = set()

    for obs in observations:
        city = obs.get("city", "?")

        # Présence des champs attendus + complétude (non nul)
        for field in REQUIRED_FIELDS:
            if field not in obs or obs[field] is None:
                anomalies.append("Champ manquant ou vide '%s' pour %s" % (field, city))

        # Cohérence : température dans une plage raisonnable
        temp = obs.get("temperature_celsius")
        if temp is not None and not (TEMPERATURE_MIN_C <= temp <= TEMPERATURE_MAX_C):
            anomalies.append("Température hors plage pour %s : %.1f°C" % (city, temp))

        # Cohérence : humidité entre 0 et 100 %
        humidity = obs.get("humidity_pct")
        if humidity is not None and not (HUMIDITY_MIN_PCT <= humidity <= HUMIDITY_MAX_PCT):
            anomalies.append("Humidité hors plage pour %s : %s%%" % (city, humidity))

        # Cohérence : précipitations non négatives
        precip = obs.get("precipitation_mm")
        if precip is not None and precip < 0:
            anomalies.append("Précipitations négatives pour %s : %.1f mm" % (city, precip))

        # Unicité : pas deux observations pour la même ville et le même horodatage
        key = (city, obs.get("observed_at"))
        if key in seen_keys:
            anomalies.append("Doublon détecté pour %s à %s" % (city, obs.get("observed_at")))
        seen_keys.add(key)

        # Fraîcheur : la mesure n'est pas trop ancienne
        observed_at = obs.get("observed_at")
        if observed_at and _is_stale(observed_at):
            anomalies.append("Donnée périmée pour %s (observed_at=%s)" % (city, observed_at))

    passed = len(anomalies) == 0

    if passed:
        logger.info("Contrôle qualité réussi — %d observations valides", len(observations))
    else:
        logger.warning("Contrôle qualité échoué — %d anomalie(s) détectée(s)", len(anomalies))
        for a in anomalies:
            logger.warning("  - %s", a)

    return {"passed": passed, "checked": len(observations), "anomalies": anomalies}


def _is_stale(observed_at: str) -> bool:
    """Retourne True si l'horodatage est plus vieux que le seuil de fraîcheur."""
    try:
        # Open-Meteo renvoie un format ISO sans timezone ; on suppose UTC
        dt = datetime.fromisoformat(observed_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        # Format inattendu = anomalie de structure, traitée comme périmée
        return True

    age = datetime.now(timezone.utc) - dt
    return age > timedelta(hours=FRESHNESS_MAX_AGE_HOURS)