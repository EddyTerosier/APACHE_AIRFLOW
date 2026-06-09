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

L'API retourne de nombreux autres champs (pression, UV, couverture nuageuse...).
Seuls les champs utiles au besoin métier sont conservés après transformation.

---

## Champs retenus — explication des choix

Après appel à l'API, la tâche `transform_weather` ne conserve que 4 champs,
auxquels elle ajoute un horodatage de traçabilité :

| Champ source API         | Champ table cible       | Unité | Pourquoi ce champ a été retenu                                 |
|--------------------------|-------------------------|-------|----------------------------------------------------------------|
| `temperature_2m`         | `temperature_celsius`   | °C    | Indicateur météo central, utile pour tout usage métier         |
| `relative_humidity_2m`   | `humidity_pct`          | %     | Complète la température pour le ressenti réel                  |
| `wind_speed_10m`         | `wind_speed_kmh`        | km/h  | Pertinent pour tout impact opérationnel lié au vent            |
| `precipitation`          | `precipitation_mm`      | mm    | Indique les précipitations actuelles                           |
| *(ajouté)*               | `fetched_at`            | —     | Horodatage de la mesure — indispensable pour la traçabilité    |

**Champs écartés :** pression atmosphérique, UV index, couverture nuageuse —
non nécessaires pour le besoin initial. Ils restent faciles à ajouter
via `API_CURRENT_FIELDS` dans le DAG.

Les colonnes sont aussi **renommées** pour que la table cible soit
indépendante des conventions de nommage de l'API source
(ex. `temperature_2m` → `temperature_celsius`).

---

## Aperçu des données préparées

Voici ce que produit la tâche `transform_weather` et ce qui est transmis
à `load_weather` via XCom (clé `clean_weather`) :

```json
[
  {
    "city": "Paris",
    "temperature_celsius": 18.4,
    "humidity_pct": 72,
    "wind_speed_kmh": 12.5,
    "precipitation_mm": 0.0,
    "fetched_at": "2026-06-01T08:00"
  },
  {
    "city": "Lyon",
    "temperature_celsius": 22.1,
    "humidity_pct": 58,
    "wind_speed_kmh": 8.3,
    "precipitation_mm": 0.0,
    "fetched_at": "2026-06-01T08:00"
  },
  {
    "city": "Marseille",
    "temperature_celsius": 25.6,
    "humidity_pct": 48,
    "wind_speed_kmh": 21.0,
    "precipitation_mm": 0.0,
    "fetched_at": "2026-06-01T08:00"
  }
]
```

Ce sont exactement les valeurs qui seront insérées ligne par ligne
dans la table `weather_data`.

---

## Ce qu'on a fait de la donnée — du brut au structuré

```
API Open-Meteo (JSON brut, tous les champs)
        ↓
  extract_weather        → récupère la réponse brute pour chaque ville
                           pousse le résultat en XCom (clé : raw_weather)
        ↓
  transform_weather      → sélectionne les 4 champs utiles
                           renomme les colonnes pour la table cible
                           ajoute l'horodatage fetched_at
                           pousse le résultat en XCom (clé : clean_weather)
        ↓
  load_weather           → reçoit les données propres
                           simule l'INSERT en base (prêt pour psycopg2)
        ↓
  Table weather_data (PostgreSQL)
```

---

## Workflow DAG

```
extract_weather  →  transform_weather  →  load_weather
```

| Tâche               | Rôle                                                              |
|---------------------|-------------------------------------------------------------------|
| `extract_weather`   | Appel API Open-Meteo, pousse la réponse brute en XCom             |
| `transform_weather` | Sélectionne, renomme et structure les champs pour la table cible  |
| `load_weather`      | Simule l'INSERT en base, prêt pour un vrai connecteur             |

---

## Passage de données entre tâches (XCom)

| Clé XCom        | Produite par        | Consommée par       | Contenu                           |
|-----------------|---------------------|---------------------|-----------------------------------|
| `raw_weather`   | `extract_weather`   | `transform_weather` | Liste brute des réponses API      |
| `clean_weather` | `transform_weather` | `load_weather`      | Liste des enregistrements propres |

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
3. Onglet "Logs"

**Consulter les XComs d'un run :**
1. Cliquer sur une task instance
2. Onglet "XCom"

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
docker compose down        # conserve les données
docker compose down -v     # supprime tout
```