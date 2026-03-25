// src/sync/audit.rs

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueType {
    FutureTimestamp,
    PastThreshold,
    NullValue,
    Duplicate,
    MissingSensor,
    OutOfRange,
}

impl std::fmt::Display for IssueType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IssueType::FutureTimestamp => write!(f, "future_timestamp"),
            IssueType::PastThreshold   => write!(f, "past_threshold"),
            IssueType::NullValue       => write!(f, "null_value"),
            IssueType::Duplicate       => write!(f, "duplicate"),
            IssueType::MissingSensor   => write!(f, "missing_sensor"),
            IssueType::OutOfRange      => write!(f, "out_of_range"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub sync_log_id: Uuid,
    pub source:      String,
    pub issue_type:  IssueType,
    pub record_id:   Option<Uuid>,
    pub details:     serde_json::Value,
    pub resolution:  String,
}

pub async fn write_audit(pool: &PgPool, entry: &AuditEntry) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO sync_audit (sync_log_id, source, issue_type, record_id, details, resolution)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        entry.sync_log_id,
        entry.source,
        entry.issue_type.to_string(),
        entry.record_id,
        entry.details,
        entry.resolution,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn start_sync_log(pool: &PgPool, source: &str) -> anyhow::Result<Uuid> {
    let row = sqlx::query!(
        r#"
        INSERT INTO sync_log (source, status)
        VALUES ($1, 'running')
        RETURNING id
        "#,
        source,
    )
    .fetch_one(pool)
    .await?;

    Ok(row.id)
}

pub async fn finish_sync_log(
    pool: &PgPool,
    log_id: Uuid,
    status: &str,
    synced: i32,
    skipped: i32,
    flagged: i32,
    conflict: i32,
    error_msg: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE sync_log
        SET
            finished_at      = now(),
            status           = $2,
            records_synced   = $3,
            records_skipped  = $4,
            records_flagged  = $5,
            records_conflict = $6,
            error_msg        = $7
        WHERE id = $1
        "#,
        log_id,
        status,
        synced,
        skipped,
        flagged,
        conflict,
        error_msg,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Construir detalles de auditoría para cada tipo de issue
pub fn details_future_timestamp(record_id: Uuid, created_at: NaiveDateTime) -> serde_json::Value {
    json!({
        "record_id":  record_id,
        "created_at": created_at.to_string(),
    })
}

pub fn details_past_threshold(record_id: Uuid, created_at: NaiveDateTime, threshold: &str) -> serde_json::Value {
    json!({
        "record_id":  record_id,
        "created_at": created_at.to_string(),
        "threshold":  threshold,
    })
}

pub fn details_null_value(record_id: Uuid, sensor_id: Uuid) -> serde_json::Value {
    json!({
        "record_id": record_id,
        "sensor_id": sensor_id,
    })
}

pub fn details_duplicate(
    record_id: Uuid,
    sensor_id: Uuid,
    created_at: NaiveDateTime,
    value_s1: f64,
    value_s2: f64,
) -> serde_json::Value {
    json!({
        "record_id":  record_id,
        "sensor_id":  sensor_id,
        "created_at": created_at.to_string(),
        "value_source1": value_s1,
        "value_source2": value_s2,
        "delta": (value_s1 - value_s2).abs(),
    })
}

pub fn details_missing_sensor(record_id: Uuid, sensor_id: Uuid) -> serde_json::Value {
    json!({
        "record_id": record_id,
        "sensor_id": sensor_id,
    })
}

pub fn details_out_of_range(
    record_id: Uuid,
    sensor_id: Uuid,
    value: f64,
    range_min: f64,
    range_max: f64,
) -> serde_json::Value {
    json!({
        "record_id": record_id,
        "sensor_id": sensor_id,
        "value":     value,
        "range_min": range_min,
        "range_max": range_max,
    })
}
