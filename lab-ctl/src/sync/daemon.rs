// src/sync/daemon.rs
//
// Sync autónomo: lee de las dos fuentes físicas, aplica reglas, inserta en la
// réplica local, y deja un log completo de cada decisión tomada.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDateTime;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::Config;
use super::audit::{self, AuditEntry};
use super::rules::{self, Reading, Verdict};

#[derive(Debug, Clone)]
pub struct SyncState {
    pub running:      bool,
    pub last_sync_at: Option<NaiveDateTime>,
    pub last_status:  String,
}

pub type SharedSyncState = Arc<RwLock<SyncState>>;

pub fn new_state() -> SharedSyncState {
    Arc::new(RwLock::new(SyncState {
        running:      false,
        last_sync_at: None,
        last_status:  "never".to_string(),
    }))
}

/// Punto de entrada del sync — se llama desde el scheduler o manualmente vía API
pub async fn run_sync(
    config: Arc<Config>,
    local_pool: PgPool,
    source1_pool: Option<PgPool>,
    source2_pool: Option<PgPool>,
    state: SharedSyncState,
) {
    {
        let mut s = state.write().await;
        if s.running {
            info!("Sync ya está corriendo, saltando...");
            return;
        }
        s.running = true;
    }

    info!("=== SYNC START ===");

    let result = do_sync(&config, &local_pool, &source1_pool, &source2_pool).await;

    let mut s = state.write().await;
    s.running = false;
    s.last_sync_at = Some(chrono::Utc::now().naive_utc());

    match result {
        Ok((synced, skipped, flagged, conflict)) => {
            s.last_status = format!(
                "success — {} insertados, {} descartados, {} flaggeados, {} conflictos",
                synced, skipped, flagged, conflict
            );
            info!(
                "=== SYNC DONE — {} insertados, {} descartados, {} flaggeados, {} conflictos ===",
                synced, skipped, flagged, conflict
            );
        }
        Err(e) => {
            s.last_status = format!("error — {}", e);
            error!("=== SYNC ERROR: {} ===", e);
        }
    }
}

async fn do_sync(
    config: &Config,
    local_pool: &PgPool,
    source1_pool: &Option<PgPool>,
    source2_pool: &Option<PgPool>,
) -> anyhow::Result<(i32, i32, i32, i32)> {
    let mut total_synced   = 0i32;
    let mut total_skipped  = 0i32;
    let mut total_flagged  = 0i32;
    let mut total_conflict = 0i32;

    // Obtener el último created_at ya sincronizado para hacer sync incremental
    let last_synced: Option<NaiveDateTime> = sqlx::query_scalar!(
        "SELECT MAX(created_at) FROM readings WHERE created_at < now()"
    )
    .fetch_one(local_pool)
    .await?;

    let since = last_synced.unwrap_or(
        chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );

    info!("Sync incremental desde: {}", since);

    // Leer readings de cada fuente desde `since`
    let readings_s1 = match source1_pool {
        Some(pool) => fetch_readings(pool, since).await.unwrap_or_else(|e| {
            error!("Error leyendo source1: {}", e);
            vec![]
        }),
        None => vec![],
    };

    let readings_s2 = match source2_pool {
        Some(pool) => fetch_readings(pool, since).await.unwrap_or_else(|e| {
            error!("Error leyendo source2: {}", e);
            vec![]
        }),
        None => vec![],
    };

    info!(
        "Leídos {} readings de source1, {} de source2",
        readings_s1.len(), readings_s2.len()
    );

    // Iniciar log de sync
    let log_id = audit::start_sync_log(local_pool, "source1+source2").await?;

    // Detectar duplicados entre fuentes (mismo sensor_id + created_at)
    let mut s2_map: HashMap<(Uuid, NaiveDateTime), Reading> = readings_s2
        .into_iter()
        .map(|r| ((r.sensor_id, r.created_at), r))
        .collect();

    let mut to_insert: Vec<Reading> = Vec::new();
    let mut audit_entries: Vec<AuditEntry> = Vec::new();

    // Procesar source1
    for reading in readings_s1 {
        let key = (reading.sensor_id, reading.created_at);

        // Resolver duplicado si existe en source2
        let (reading, conflict_audit) = if let Some(s2_reading) = s2_map.remove(&key) {
            total_conflict += 1;
            let (winner, audit) = rules::resolve_duplicate(&reading, &s2_reading, config, log_id);
            if let Some(a) = audit { audit_entries.push(a); }
            (winner, true)
        } else {
            (reading, false)
        };

        // Aplicar reglas al reading resultante
        match apply_rules(&reading, config, "source1", log_id, local_pool, &mut audit_entries).await? {
            Verdict::Insert  => to_insert.push(reading),
            Verdict::Discard => total_skipped += 1,
            Verdict::Flag    => {
                total_flagged += 1;
                to_insert.push(reading); // se inserta pero quedó en audit
            }
        }

    }

    // Procesar lo que quedó en source2 (no tenía par en source1)
    for (_, reading) in s2_map {
        match apply_rules(&reading, config, "source2", log_id, local_pool, &mut audit_entries).await? {
            Verdict::Insert  => to_insert.push(reading),
            Verdict::Discard => total_skipped += 1,
            Verdict::Flag    => {
                total_flagged += 1;
                to_insert.push(reading);
            }
        }
    }

    // Insertar en batch en la réplica local
    for reading in &to_insert {
        let insert_result = sqlx::query!(
            r#"
            INSERT INTO readings (id, sensor_id, value, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO NOTHING
            "#,
            reading.id,
            reading.sensor_id,
            reading.value.map(|v| sqlx::types::BigDecimal::from(v as i64)),
            reading.created_at,
        )
        .execute(local_pool)
        .await;

        match insert_result {
            Ok(r) if r.rows_affected() > 0 => total_synced += 1,
            Ok(_)  => {} // ya existía (ON CONFLICT DO NOTHING)
            Err(e) => error!("Error insertando reading {}: {}", reading.id, e),
        }
    }

    // Escribir audit entries
    for entry in &audit_entries {
        if let Err(e) = audit::write_audit(local_pool, entry).await {
            error!("Error escribiendo audit: {}", e);
        }
    }

    // Cerrar log de sync
    audit::finish_sync_log(
        local_pool,
        log_id,
        "success",
        total_synced,
        total_skipped,
        total_flagged,
        total_conflict,
        None,
    ).await?;

    Ok((total_synced, total_skipped, total_flagged, total_conflict))
}

async fn fetch_readings(pool: &PgPool, since: NaiveDateTime) -> anyhow::Result<Vec<Reading>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            id,
            sensor_id,
            value::float8 AS value,
            created_at
        FROM readings
        WHERE created_at > $1
        ORDER BY created_at ASC
        LIMIT 50000
        "#,
        since,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().filter_map(|r| {
        Some(Reading {
            id:         r.id,
            sensor_id:  r.sensor_id?,
            value:      r.value,
            created_at: r.created_at,
        })
    }).collect())
}

async fn apply_rules(
    reading: &Reading,
    config: &Config,
    source: &str,
    log_id: Uuid,
    local_pool: &PgPool,
    audit_entries: &mut Vec<AuditEntry>,
) -> anyhow::Result<Verdict> {
    // Orden de prioridad de reglas
    if let Some(r) = rules::check_future_timestamp(reading, config, source, log_id) {
        if let Some(a) = r.audit { audit_entries.push(a); }
        if r.verdict == Verdict::Discard { return Ok(Verdict::Discard); }
    }

    if let Some(r) = rules::check_past_threshold(reading, config, source, log_id) {
        if let Some(a) = r.audit { audit_entries.push(a); }
        if r.verdict == Verdict::Discard { return Ok(Verdict::Discard); }
    }

    if let Some(r) = rules::check_null_value(reading, config, source, log_id) {
        if let Some(a) = r.audit { audit_entries.push(a); }
        if r.verdict == Verdict::Discard { return Ok(Verdict::Discard); }
    }

    if let Some(r) = rules::check_missing_sensor(reading, config, source, log_id, local_pool).await? {
        if let Some(a) = r.audit { audit_entries.push(a); }
        if r.verdict == Verdict::Discard { return Ok(Verdict::Discard); }
    }

    // Out of range es siempre Flag (no descarta por defecto)
    if let Some(r) = rules::check_out_of_range(reading, config, source, log_id, local_pool).await? {
        if let Some(a) = r.audit { audit_entries.push(a); }
        if r.verdict == Verdict::Flag { return Ok(Verdict::Flag); }
    }

    Ok(Verdict::Insert)
}
