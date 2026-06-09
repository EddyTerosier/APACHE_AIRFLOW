# weather_pipeline

Pipeline Airflow d'ingestion quotidienne des données météo via Open-Meteo.

---

## Objectif

Récupérer chaque jour la météo de Paris, Lyon et Marseille, sélectionner
les champs utiles pour le besoin métier, et les charger en base de données.

---

## Données source — Open-Meteo

**API :** https://open-meteo.com
**Accès :** gratuit, sans clé API, sans inscription
**Méthode :** GET avec coordonnées GPS en paramètres

### Exemple d'appel (Paris)

```
GET https://api.open-meteo.com/v1/forecast
  ?latitude=48.8566
  &longitude=2.3522
  &current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation
```

### Réponse brute retournée par l'API

```json
{
  "current": {
    "time": "2026-06-01T08:00",
    "temperature_2m": 18.4,
    "relative_humidity_2m": 72,
    "wind_speed_10m": 12.5,
    "precipitation": 0.0
  }
}
```

L'API retourne de nombreux autres champs (pression, nuages, rayonnement UV...).
Seuls les champs utiles au besoin métier sont conservés après transformation.

---

## Champs retenus et justification

| Champ source API          | Champ table cible       | Unité  | Justification                                      |
|---------------------------|-------------------------|--------|----------------------------------------------------|
| `temperature_2m`          | `temperature_celsius`   | °C     | Indicateur météo principal, utile pour tout besoin métier |
| `relative_humidity_2m`    | `humidity_pct`          | %      | Complète la température pour le ressenti réel      |
| `wind_speed_10m`          | `wind_speed_kmh`        | km/h   | Pertinent pour les impacts opérationnels           |
| `precipitation`           | `precipitation_mm`      | mm     | Indicateur de précipitations actuelles             |
| *(ajouté en transform)*   | `fetched_at`            | —      | Horodatage de la mesure, indispensable pour la traçabilité en base |

**Champs non retenus :** pression atmosphérique, UV index, couverture nuageuse —
non demandés dans le besoin initial, ajoutables facilement via `API_CURRENT_FIELDS`.

---

## Ce qu'on en a fait — du brut au structuré

```
API Open-Meteo (JSON brut)
        ↓
  extract_weather        → récupère la réponse brute pour chaque ville
        ↓  [XCom: raw_weather]
  transform_weather      → sélectionne les champs, renomme les colonnes,
                           ajoute l'horodatage
        ↓  [XCom: clean_weather]
  load_weather           → simule l'INSERT en base (prêt pour psycopg2)
        ↓
  Table weather_data (PostgreSQL)
```

La séparation extract / transform / load garantit que chaque tâche a
une responsabilité unique et peut être relancée indépendamment en cas d'échec.

---

## Workflow DAG

```
extract_weather  →  transform_weather  →  load_weather
```

| Tâche              | Rôle                                                              |
|--------------------|-------------------------------------------------------------------|
| `extract_weather`  | Appel API Open-Meteo pour chaque ville, pousse le brut en XCom   |
| `transform_weather`| Sélectionne les champs utiles, renomme, structure pour la table   |
| `load_weather`     | Simule l'INSERT en base (logs), prêt pour un vrai connecteur      |

---

## Passage de données entre tâches (XCom)

Les tâches s'échangent de petits objets via XCom :

| Clé XCom        | Produite par       | Consommée par      | Contenu                              |
|-----------------|--------------------|--------------------|--------------------------------------|
| `raw_weather`   | `extract_weather`  | `transform_weather`| Liste brute des réponses API         |
| `clean_weather` | `transform_weather`| `load_weather`     | Liste des enregistrements nettoyés   |

> XCom est utilisé ici parce que le volume est faible (3 villes × 5 champs).
> Pour un volume important, on écrirait dans un fichier ou MinIO et on
> transmettrait seulement le chemin via XCom.

---

## Table cible

```sql
CREATE TABLE weather_data (
    city                TEXT        NOT NULL,
    temperature_celsius FLOAT       NOT NULL,
    humidity_pct        INTEGER     NOT NULL,
    wind_speed_kmh      FLOAT       NOT NULL,
    precipitation_mm    FLOAT       NOT NULL,
    fetched_at          TIMESTAMP   NOT NULL
);
```

---

## Prérequis

- Docker et Docker Compose installés
- Aucune clé API requise

---

## Lancement

```bash
# 1. Créer les dossiers nécessaires à Airflow
mkdir -p ./dags ./logs ./plugins

# 2. Créer le fichier d'environnement
echo "AIRFLOW_UID=$(id -u)" > .env

# 3. Démarrer Airflow
docker compose up -d

# 4. Copier le DAG dans le dossier surveillé par Airflow
cp weather_pipeline_dag.py ./dags/
```

Airflow détecte automatiquement les fichiers déposés dans `./dags/`
(délai de quelques secondes à une minute).

---

## Interface web

URL : http://localhost:8080
Login : `admin` / `admin`

**Lancer le DAG manuellement :**
1. Ouvrir http://localhost:8080
2. Cliquer sur `weather_pipeline`
3. Cliquer sur ▶ "Trigger DAG"

**Consulter les logs d'une tâche :**
1. Cliquer sur un run dans la vue "Grid"
2. Cliquer sur une tâche (ex. `extract_weather`)
3. Ouvrir l'onglet "Logs"

**Consulter les XComs d'un run :**
1. Cliquer sur un run
2. Onglet "XCom" sur une task instance

---

## Structure du projet

```
weather_pipeline/
├── docker-compose.yml          # Environnement Airflow
├── weather_pipeline_dag.py     # Fichier DAG Python
├── README.md                   # Ce fichier
├── dags/                       # Dossier surveillé par Airflow
├── logs/                       # Logs des tâches
└── plugins/                    # Plugins Airflow (vide pour ce TP)
```

---

## Arrêter l'environnement

```bash
# Arrêter (conserve les données)
docker compose down

# Arrêter et tout supprimer
docker compose down -v
```

---
### Si `requests` est manquant

Créer un fichier `requirements.txt` à la racine :
```
requests
```
Puis ajouter dans le service `airflow-common` du `docker-compose.yml` :
```yaml
volumes:
  - ./requirements.txt:/requirements.txt
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: "requests"
```