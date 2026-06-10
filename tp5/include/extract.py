"""Logique d'extraction des données depuis l'API Open-Meteo."""

import logging

import requests

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
API_CURRENT_FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"

# Timeout réseau (secondes) pour éviter qu'un appel API reste bloqué
API_TIMEOUT = 10


def fetch_city_payload(city: dict) -> dict:
    """Appelle Open-Meteo pour une ville et retourne le payload JSON complet.

    Lève requests.HTTPError si l'API répond avec un statut d'erreur (4xx/5xx),
    requests.Timeout si l'appel dépasse API_TIMEOUT.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": API_CURRENT_FIELDS,
    }

    logger.info("Appel API Open-Meteo pour %s", city["name"])
    response = requests.get(OPEN_METEO_URL, params=params, timeout=API_TIMEOUT)
    response.raise_for_status()

    return response.json()


def fetch_all_cities(cities: list[dict]) -> list[dict]:
    """Récupère les payloads bruts pour toutes les villes.

    Retourne une liste d'objets {city, latitude, longitude, payload}.
    """
    payloads = []
    for city in cities:
        payload = fetch_city_payload(city)
        payloads.append({
            "city": city["name"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "payload": payload,
        })
        logger.info("Payload reçu pour %s", city["name"])

    logger.info("Extraction terminée — %d villes", len(payloads))
    return payloads