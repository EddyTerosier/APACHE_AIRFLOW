# weather_pipeline

Pipeline Airflow de récupération quotidienne des données météo.

Récupère chaque jour la météo de Paris, Lyon et Marseille via l'API
Open-Meteo, valide les données reçues, puis simule leur chargement en base.

---

## Workflow

```
fetch_weather  →  validate_weather  →  load_weather
```

| Tâche              | Rôle                                                        |
|--------------------|-------------------------------------------------------------|
| `fetch_weather`    | Appel API Open-Meteo pour chaque ville                      |
| `validate_weather` | Vérifie la présence et la cohérence des champs reçus        |
| `load_weather`     | Simule l'écriture en base (logs INSERT)                     |

---

## API utilisée

**Open-Meteo** — gratuite, sans inscription, sans clé API.
Documentation : https://open-meteo.com

Exemple d'appel pour Paris :
```
GET https://api.open-meteo.com/v1/forecast
  ?latitude=48.8566
  &longitude=2.3522
  &current=temperature_2m,relative_humidity_2m,wind_speed_10m
```

Exemple de réponse :
```json
{
  "current": {
    "temperature_2m": 18.4,
    "relative_humidity_2m": 72,
    "wind_speed_10m": 12.5
  }
}
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

# 2. Créer le fichier d'environnement (UID utilisateur pour Docker)
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
Login : `admin`
Mot de passe : `admin`

**Lancer le DAG manuellement :**
1. Ouvrir http://localhost:8080
2. Cliquer sur `weather_pipeline` dans la liste des DAGs
3. Cliquer sur le bouton ▶ "Trigger DAG" en haut à droite

**Consulter les logs d'une tâche :**
1. Cliquer sur un run dans la vue "Grid"
2. Cliquer sur une tâche (ex. `fetch_weather`)
3. Ouvrir l'onglet "Logs"

---

## Structure du projet

```
weather_pipeline/
├── docker-compose.yml          # Environnement Airflow (Webserver + Scheduler + Postgres)
├── weather_pipeline_dag.py     # Fichier DAG Python
├── README.md                   # Ce fichier
├── dags/                       # Dossier surveillé par Airflow (généré au lancement)
├── logs/                       # Logs des tâches (généré au lancement)
└── plugins/                    # Plugins Airflow (pour plus tard)
```

---

## Arrêter l'environnement

```bash
# Arrêter les conteneurs (conserve les données)
docker compose down

# Arrêter et supprimer toutes les données (repart de zéro)
docker compose down -v
```

---
