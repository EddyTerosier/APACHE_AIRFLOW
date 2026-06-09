# TP 2B — Pipeline complet API → transformation → PostgreSQL

Pipeline Airflow orchestrant l'ingestion quotidienne des données météo
Open-Meteo selon une architecture en couches **bronze → silver → gold**,
avec traçabilité complète dans `technical.ingestion_runs`.

---

## Structure du dossier

```
tp2b/
├── dags/
│   └── weather_pipeline.py   # DAG Airflow
├── sql/
│   └── init_db.sql           # Création des schémas et tables
├── docker-compose.yml        # Airflow + 2 bases PostgreSQL
└── README.md
```

---

## Workflow DAG

```
extract_api_payload
        ↓
store_raw_payload        → bronze.raw_weather_payloads
        ↓
transform_to_silver      → silver.weather_observations
        ↓
load_gold_metrics        → gold.weather_daily_city
        ↓
log_ingestion_run        → technical.ingestion_runs  (toujours exécutée)
```

| Tâche                  | Rôle                                                                        |
|------------------------|-----------------------------------------------------------------------------|
| `extract_api_payload`  | Appel Open-Meteo pour chaque ville, pousse les payloads bruts en XCom       |
| `store_raw_payload`    | Insère le JSON brut dans `bronze.raw_weather_payloads`                      |
| `transform_to_silver`  | Sélectionne, normalise et charge dans `silver.weather_observations`         |
| `load_gold_metrics`    | Calcule les agrégats journaliers et fait un upsert dans `gold.weather_daily_city` |
| `log_ingestion_run`    | Trace l'exécution dans `technical.ingestion_runs` (trigger `all_done`)      |

---

## Architecture base de données

```
weather_db
├── technical
│   └── ingestion_runs          ← suivi de chaque run DAG
├── bronze
│   └── raw_weather_payloads    ← payload JSON brut de l'API
├── silver
│   └── weather_observations    ← données nettoyées et typées
└── gold
    └── weather_daily_city      ← agrégats journaliers par ville
```

| Couche      | Rôle                                  | Ce qu'Airflow doit garantir              |
|-------------|---------------------------------------|------------------------------------------|
| `bronze`    | Conserver la preuve source            | Horodatage, run_id, non-perte            |
| `silver`    | Nettoyer et standardiser              | Typage, schéma conforme, run_id          |
| `gold`      | Servir le métier                      | Idempotence (upsert), cohérence métier   |
| `technical` | Traçabilité des exécutions            | Toujours écrit, même en cas d'échec      |

---

## Données source — Open-Meteo

**API :** https://open-meteo.com — gratuite, sans clé, sans inscription.

Exemple d'appel (Paris) :
```
GET https://api.open-meteo.com/v1/forecast
  ?latitude=48.8566&longitude=2.3522
  &current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation
```

Réponse brute stockée telle quelle en bronze :
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

---

## Champs retenus (bronze → silver)

| Champ API                | Colonne silver              | Unité | Justification                            |
|--------------------------|-----------------------------|-------|------------------------------------------|
| `temperature_2m`         | `temperature_celsius`       | °C    | Indicateur météo principal               |
| `relative_humidity_2m`   | `humidity_pct`              | %     | Complète la température pour le ressenti |
| `wind_speed_10m`         | `wind_speed_kmh`            | km/h  | Impact opérationnel lié au vent          |
| `precipitation`          | `precipitation_mm`          | mm    | Précipitations actuelles                 |
| `time` (API)             | `observed_at`               | —     | Horodatage de la mesure                  |

---

## Aperçu des données par couche

**bronze.raw_weather_payloads** — JSON brut conservé :
```
run_id                          | city      | payload_json
--------------------------------|-----------|----------------------------------------------
scheduled__2026-06-01T06:00:00  | Paris     | {"current": {"time": "2026-06-01T08:00", ...}}
scheduled__2026-06-01T06:00:00  | Lyon      | {"current": {"time": "2026-06-01T08:00", ...}}
scheduled__2026-06-01T06:00:00  | Marseille | {"current": {"time": "2026-06-01T08:00", ...}}
```

**silver.weather_observations** — données nettoyées :
```
city      | observed_at         | temperature_celsius | humidity_pct | wind_speed_kmh | precipitation_mm
----------|---------------------|---------------------|--------------|----------------|------------------
Paris     | 2026-06-01 08:00:00 |                18.4 |           72 |           12.5 |              0.0
Lyon      | 2026-06-01 08:00:00 |                22.1 |           58 |            8.3 |              0.0
Marseille | 2026-06-01 08:00:00 |                25.6 |           48 |           21.0 |              0.0
```

**gold.weather_daily_city** — agrégats journaliers :
```
city      | observation_date | avg_temperature | max_wind_speed | total_precipitation
----------|------------------|-----------------|----------------|---------------------
Paris     | 2026-06-01       |            18.4 |           12.5 |                 0.0
Lyon      | 2026-06-01       |            22.1 |            8.3 |                 0.0
Marseille | 2026-06-01       |            25.6 |           21.0 |                 0.0
```

---

## Étapes de mise en place

### 1. Démarrer l'environnement

```bash
cd tp2b

mkdir -p ./dags ./logs ./plugins
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up -d
```

### 2. Vérifier que les tables ont été créées

Le fichier `sql/init_db.sql` est exécuté automatiquement par PostgreSQL
au premier démarrage via `docker-entrypoint-initdb.d`.

Pour vérifier :
```bash
docker compose exec postgres-metier psql -U weather -d weather_db -c "\dn"
```

Tu dois voir les 4 schémas : `technical`, `bronze`, `silver`, `gold`.

```bash
docker compose exec postgres-metier psql -U weather -d weather_db \
  -c "\dt technical.* \dt bronze.* \dt silver.* \dt gold.*"
```

Si les tables sont absentes (volume déjà existant), forcer la réinitialisation :
```bash
docker compose down -v
docker compose up -d
```

### 3. Configurer la connexion Airflow → PostgreSQL métier

Dans l'UI Airflow (http://localhost:8080) :

**Admin → Connections → +**

| Champ           | Valeur             |
|-----------------|--------------------|
| Connection Id   | `weather_postgres` |
| Connection Type | `Postgres`         |
| Host            | `postgres-metier`  |
| Database        | `weather_db`       |
| Login           | `weather`          |
| Password        | `weather`          |
| Port            | `5432`             |

> Le Host est `postgres-metier` (nom du service Docker), pas `localhost`.

### 4. Copier le DAG

```bash
cp dags/weather_pipeline.py ./dags/
```

Attendre ~30 secondes puis actualiser l'UI.

### 5. Lancer le DAG

**http://localhost:8080 → weather_pipeline → ▶ Trigger DAG**

---

## Requêtes de vérification après un run

### Contenu bronze — payload brut reçu
```sql
SELECT run_id, city, ingested_at,
       payload_json -> 'current' -> 'temperature_2m' AS temperature_raw
FROM bronze.raw_weather_payloads
ORDER BY ingested_at DESC
LIMIT 6;
```

### Contenu silver — données nettoyées
```sql
SELECT city, observed_at, temperature_celsius,
       humidity_pct, wind_speed_kmh, precipitation_mm
FROM silver.weather_observations
ORDER BY observed_at DESC, city
LIMIT 10;
```

### Contenu gold — agrégats journaliers
```sql
SELECT city, observation_date,
       avg_temperature, max_wind_speed, total_precipitation
FROM gold.weather_daily_city
ORDER BY observation_date DESC, city;
```

### Suivi des runs — traçabilité complète
```sql
SELECT source, data_interval_start, status,
       records_received, records_inserted, ended_at
FROM technical.ingestion_runs
ORDER BY created_at DESC
LIMIT 5;
```

### Requête analytique — température moyenne par ville sur la semaine
```sql
SELECT
    city,
    COUNT(*)                                          AS nb_jours,
    ROUND(AVG(avg_temperature)::numeric, 1)           AS temp_moyenne,
    ROUND(MAX(max_wind_speed)::numeric, 1)            AS vent_max,
    ROUND(SUM(total_precipitation)::numeric, 1)       AS precip_totale
FROM gold.weather_daily_city
WHERE observation_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY city
ORDER BY city;
```

---

## Paramétrage — Variable `weather_cities`

Pour modifier les villes sans toucher au code :

**Admin → Variables → +**

| Champ | Valeur           |
|-------|------------------|
| Key   | `weather_cities` |
| Val   | JSON ci-dessous  |

```json
[
  {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
  {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
  {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
  {"name": "Bordeaux",  "latitude": 44.8378, "longitude": -0.5792}
]
```

---

## Arrêter l'environnement

```bash
docker compose down        # conserve les données
docker compose down -v     # supprime tout et repart de zéro
```