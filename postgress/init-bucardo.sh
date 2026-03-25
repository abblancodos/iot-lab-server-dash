#!/bin/bash
set -e

# Crear base de datos para Wiki.js
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE wiki;
EOSQL

echo "Wiki database created."

# Instalar schema de Bucardo
bucardo install --batch \
    --dbhost=localhost \
    --dbport=5432 \
    --dbname="$POSTGRES_DB" \
    --dbuser="$POSTGRES_USER" \
    --dbpass="$POSTGRES_PASSWORD" || true

echo "Bucardo schema installed."