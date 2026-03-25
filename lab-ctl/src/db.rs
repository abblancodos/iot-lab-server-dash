// src/db.rs

use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::info;

use crate::config::SourceConfig;

pub async fn connect_local() -> anyhow::Result<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL no definida"))?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await?;

    info!("Conectado a PostgreSQL local");
    Ok(pool)
}

pub async fn connect_source(source: &SourceConfig, name: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&source.connection_string())
        .await
        .map_err(|e| anyhow::anyhow!("No se pudo conectar a {}: {}", name, e))?;

    info!("Conectado a fuente {}", name);
    Ok(pool)
}

/// Inicializar tablas de auditoría en el PG local si no existen
pub async fn init_schema(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

        CREATE TABLE IF NOT EXISTS sync_log (
            id             uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            started_at     timestamp NOT NULL DEFAULT now(),
            finished_at    timestamp,
            source         text NOT NULL,
            status         text NOT NULL,
            records_synced integer NOT NULL DEFAULT 0,
            records_skipped integer NOT NULL DEFAULT 0,
            records_flagged integer NOT NULL DEFAULT 0,
            records_conflict integer NOT NULL DEFAULT 0,
            error_msg      text
        );

        CREATE TABLE IF NOT EXISTS sync_audit (
            id          uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            synced_at   timestamp NOT NULL DEFAULT now(),
            sync_log_id uuid REFERENCES sync_log(id) ON DELETE SET NULL,
            source      text NOT NULL,
            issue_type  text NOT NULL,
            table_name  text NOT NULL DEFAULT 'readings',
            record_id   uuid,
            details     jsonb,
            resolved    boolean NOT NULL DEFAULT false,
            resolved_at timestamp,
            resolution  text
        );

        CREATE INDEX IF NOT EXISTS sync_audit_issue_type_idx ON sync_audit(issue_type);
        CREATE INDEX IF NOT EXISTS sync_audit_resolved_idx ON sync_audit(resolved);
        CREATE INDEX IF NOT EXISTS sync_audit_synced_at_idx ON sync_audit(synced_at DESC);
    "#)
    .execute(pool)
    .await?;

    info!("Schema de auditoría inicializado");
    Ok(())
}
