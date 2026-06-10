-- ============================================================
-- weather_pipeline (TP5) — Initialisation base de données
-- Schémas : technical | bronze | silver | gold
-- ============================================================

CREATE SCHEMA IF NOT EXISTS technical;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ── technical.ingestion_runs ─────────────────────────────────
-- Trace globale de chaque exécution du DAG.
CREATE TABLE IF NOT EXISTS technical.ingestion_runs (
    id                   SERIAL      PRIMARY KEY,
    run_id               TEXT        NOT NULL,
    source               TEXT        NOT NULL,
    data_interval_start  TIMESTAMP,
    data_interval_end    TIMESTAMP,
    started_at           TIMESTAMP,
    ended_at             TIMESTAMP,
    status               TEXT        NOT NULL,
    records_received     INTEGER     DEFAULT 0,
    records_inserted     INTEGER     DEFAULT 0,
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);


-- ── technical.data_quality_results ───────────────────────────
-- Résultat des contrôles qualité par run (complétude, unicité, etc.).
CREATE TABLE IF NOT EXISTS technical.data_quality_results (
    id              SERIAL      PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    status          TEXT        NOT NULL,        -- 'passed' ou 'failed'
    records_checked INTEGER     DEFAULT 0,
    anomaly_count   INTEGER     DEFAULT 0,
    detail          TEXT,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);


-- ── bronze.raw_weather_payloads ──────────────────────────────
-- Payload JSON brut tel que renvoyé par l'API (preuve source).
CREATE TABLE IF NOT EXISTS bronze.raw_weather_payloads (
    id           SERIAL      PRIMARY KEY,
    run_id       TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    latitude     FLOAT       NOT NULL,
    longitude    FLOAT       NOT NULL,
    payload_json JSONB       NOT NULL,
    ingested_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_run_id ON bronze.raw_weather_payloads (run_id);


-- ── silver.weather_observations ──────────────────────────────
-- Observations nettoyées et typées.
-- Contrainte UNIQUE (city, observed_at) : garantit l'unicité métier.
CREATE TABLE IF NOT EXISTS silver.weather_observations (
    id                   SERIAL      PRIMARY KEY,
    city                 TEXT        NOT NULL,
    observed_at          TIMESTAMP   NOT NULL,
    temperature_celsius  FLOAT       NOT NULL,
    humidity_pct         INTEGER     NOT NULL,
    wind_speed_kmh       FLOAT       NOT NULL,
    precipitation_mm     FLOAT       NOT NULL,
    run_id               TEXT        NOT NULL,
    inserted_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_silver_city_observed UNIQUE (city, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_silver_city ON silver.weather_observations (city);
CREATE INDEX IF NOT EXISTS idx_silver_run_id ON silver.weather_observations (run_id);


-- ── gold.weather_daily_city ───────────────────────────────────
-- Agrégats journaliers par ville. UNIQUE pour l'idempotence des upserts.
CREATE TABLE IF NOT EXISTS gold.weather_daily_city (
    id                   SERIAL      PRIMARY KEY,
    city                 TEXT        NOT NULL,
    observation_date     DATE        NOT NULL,
    avg_temperature      FLOAT       NOT NULL,
    max_wind_speed       FLOAT       NOT NULL,
    total_precipitation  FLOAT       NOT NULL,
    run_id               TEXT        NOT NULL,
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_gold_city_date UNIQUE (city, observation_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_city ON gold.weather_daily_city (city);