// src/routes/sync.rs

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppState;
use crate::sync::daemon;

// ── Trigger manual ────────────────────────────────────────────────────────────

pub async fn trigger_sync(State(state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let sync_state = state.sync_state.read().await;
    if sync_state.running {
        return Ok(Json(serde_json::json!({ "status": "already_running" })));
    }
    drop(sync_state);

    // Lanzar sync en background
    let config      = state.config.clone();
    let local_pool  = state.local_pool.clone();
    let s1_pool     = state.source1_pool.clone();
    let s2_pool     = state.source2_pool.clone();
    let sync_state  = state.sync_state.clone();

    tokio::spawn(async move {
        daemon::run_sync(config, local_pool, s1_pool, s2_pool, sync_state).await;
    });

    Ok(Json(serde_json::json!({ "status": "started" })))
}

// ── Estado del sync ───────────────────────────────────────────────────────────

pub async fn sync_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let s = state.sync_state.read().await;
    Json(serde_json::json!({
        "running":      s.running,
        "last_sync_at": s.last_sync_at.map(|t| t.to_string()),
        "last_status":  s.last_status,
    }))
}

// ── Historial de syncs ────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LogQuery {
    limit:  Option<i64>,
    offset: Option<i64>,
}

#[derive(Serialize)]
pub struct SyncLogEntry {
    pub id:               Uuid,
    pub started_at:       String,
    pub finished_at:      Option<String>,
    pub source:           String,
    pub status:           String,
    pub records_synced:   i32,
    pub records_skipped:  i32,
    pub records_flagged:  i32,
    pub records_conflict: i32,
    pub error_msg:        Option<String>,
}

pub async fn sync_logs(
    State(state): State<AppState>,
    Query(q): Query<LogQuery>,
) -> Result<Json<Vec<SyncLogEntry>>, StatusCode> {
    let limit  = q.limit.unwrap_or(20).min(100);
    let offset = q.offset.unwrap_or(0);

    let rows = sqlx::query!(
        r#"
        SELECT
            id, started_at, finished_at, source, status,
            records_synced, records_skipped, records_flagged, records_conflict,
            error_msg
        FROM sync_log
        ORDER BY started_at DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.local_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.into_iter().map(|r| SyncLogEntry {
        id:               r.id,
        started_at:       r.started_at.to_string(),
        finished_at:      r.finished_at.map(|t| t.to_string()),
        source:           r.source,
        status:           r.status,
        records_synced:   r.records_synced,
        records_skipped:  r.records_skipped,
        records_flagged:  r.records_flagged,
        records_conflict: r.records_conflict,
        error_msg:        r.error_msg,
    }).collect()))
}

// ── Audit ─────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct AuditQuery {
    issue_type: Option<String>,
    source:     Option<String>,
    resolved:   Option<bool>,
    limit:      Option<i64>,
    offset:     Option<i64>,
}

#[derive(Serialize)]
pub struct AuditEntryResponse {
    pub id:         Uuid,
    pub synced_at:  String,
    pub source:     String,
    pub issue_type: String,
    pub record_id:  Option<Uuid>,
    pub details:    serde_json::Value,
    pub resolved:   bool,
    pub resolution: Option<String>,
}

pub async fn audit_list(
    State(state): State<AppState>,
    Query(q): Query<AuditQuery>,
) -> Result<Json<Vec<AuditEntryResponse>>, StatusCode> {
    let limit  = q.limit.unwrap_or(50).min(200);
    let offset = q.offset.unwrap_or(0);

    let rows = sqlx::query!(
        r#"
        SELECT id, synced_at, source, issue_type, record_id, details, resolved, resolution
        FROM sync_audit
        WHERE
            ($1::text IS NULL OR issue_type = $1)
            AND ($2::text IS NULL OR source = $2)
            AND ($3::boolean IS NULL OR resolved = $3)
        ORDER BY synced_at DESC
        LIMIT $4 OFFSET $5
        "#,
        q.issue_type,
        q.source,
        q.resolved,
        limit,
        offset,
    )
    .fetch_all(&state.local_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.into_iter().map(|r| AuditEntryResponse {
        id:         r.id,
        synced_at:  r.synced_at.to_string(),
        source:     r.source,
        issue_type: r.issue_type,
        record_id:  r.record_id,
        details:    r.details.unwrap_or(serde_json::Value::Null),
        resolved:   r.resolved,
        resolution: r.resolution,
    }).collect()))
}

#[derive(Serialize)]
pub struct AuditSummary {
    pub issue_type: String,
    pub count:      i64,
    pub unresolved: i64,
}

pub async fn audit_summary(
    State(state): State<AppState>,
) -> Result<Json<Vec<AuditSummary>>, StatusCode> {
    let rows = sqlx::query!(
        r#"
        SELECT
            issue_type,
            COUNT(*)                            AS "count!: i64",
            COUNT(*) FILTER (WHERE NOT resolved) AS "unresolved!: i64"
        FROM sync_audit
        GROUP BY issue_type
        ORDER BY 2 DESC
        "#,
    )
    .fetch_all(&state.local_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows.into_iter().map(|r| AuditSummary {
        issue_type: r.issue_type,
        count:      r.count,
        unresolved: r.unresolved,
    }).collect()))
}