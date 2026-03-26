#!/bin/bash
set -e

# ── Crear base de datos para Wiki.js ─────────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE wiki;
EOSQL
echo "Wiki database created."

# ── Schema de AgroDash ────────────────────────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

    CREATE TABLE IF NOT EXISTS boxes (
        id   uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
        name text NOT NULL,
        CONSTRAINT boxes_name_key UNIQUE (name)
    );

    CREATE TABLE IF NOT EXISTS sensors (
        id            uuid    PRIMARY KEY DEFAULT uuid_generate_v4(),
        box_id        uuid    REFERENCES boxes(id) ON DELETE CASCADE,
        sensor_number integer NOT NULL,
        type          text    NOT NULL,
        CONSTRAINT sensors_box_id_sensor_number_type_key UNIQUE (box_id, sensor_number, type)
    );

    CREATE TABLE IF NOT EXISTS readings (
        id         uuid      PRIMARY KEY DEFAULT uuid_generate_v4(),
        sensor_id  uuid      REFERENCES sensors(id) ON DELETE CASCADE,
        value      numeric,
        created_at timestamp NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS readings_sensor_id_idx  ON readings(sensor_id);
    CREATE INDEX IF NOT EXISTS readings_created_at_idx ON readings(created_at DESC);

    CREATE TABLE IF NOT EXISTS alert_ranges (
        id            uuid    PRIMARY KEY DEFAULT uuid_generate_v4(),
        box_name      text    NOT NULL,
        sensor_number integer NOT NULL,
        sensor_type   text    NOT NULL,
        range_min     numeric NOT NULL,
        range_max     numeric NOT NULL,
        updated_at    timestamp DEFAULT now(),
        CONSTRAINT alert_ranges_box_name_sensor_number_sensor_type_key
            UNIQUE (box_name, sensor_number, sensor_type)
    );
EOSQL
echo "AgroDash schema created."

# ── Schema de lab-ctl (audit y sync log) ─────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS sync_log (
        id               uuid      PRIMARY KEY DEFAULT uuid_generate_v4(),
        started_at       timestamp NOT NULL DEFAULT now(),
        finished_at      timestamp,
        source           text      NOT NULL,
        status           text      NOT NULL,
        records_synced   integer   NOT NULL DEFAULT 0,
        records_skipped  integer   NOT NULL DEFAULT 0,
        records_flagged  integer   NOT NULL DEFAULT 0,
        records_conflict integer   NOT NULL DEFAULT 0,
        error_msg        text
    );

    CREATE TABLE IF NOT EXISTS sync_audit (
        id          uuid      PRIMARY KEY DEFAULT uuid_generate_v4(),
        synced_at   timestamp NOT NULL DEFAULT now(),
        sync_log_id uuid      REFERENCES sync_log(id) ON DELETE SET NULL,
        source      text      NOT NULL,
        issue_type  text      NOT NULL,
        table_name  text      NOT NULL DEFAULT 'readings',
        record_id   uuid,
        details     jsonb,
        resolved    boolean   NOT NULL DEFAULT false,
        resolved_at timestamp,
        resolution  text
    );

    CREATE INDEX IF NOT EXISTS sync_audit_issue_type_idx ON sync_audit(issue_type);
    CREATE INDEX IF NOT EXISTS sync_audit_resolved_idx   ON sync_audit(resolved);
    CREATE INDEX IF NOT EXISTS sync_audit_synced_at_idx  ON sync_audit(synced_at DESC);
EOSQL
echo "lab-ctl audit schema created."

# ── Instalar schema de Bucardo ────────────────────────────────────────────────
bucardo install --batch \
    --dbhost=localhost \
    --dbport=5432 \
    --dbname="$POSTGRES_DB" \
    --dbuser="$POSTGRES_USER" \
    --dbpass="$POSTGRES_PASSWORD" || true

echo "Bucardo schema installed."