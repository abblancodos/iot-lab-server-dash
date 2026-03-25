// src/routes/health.rs

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status:       String,
    pub local_db:     String,
    pub source1_db:   String,
    pub source2_db:   String,
    pub sync_running: bool,
    pub last_sync_at: Option<String>,
    pub last_status:  String,
}

pub async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, StatusCode> {
    let local_ok = sqlx::query("SELECT 1")
        .execute(&state.local_pool)
        .await
        .is_ok();

    let s1_ok = match &state.source1_pool {
        Some(p) => sqlx::query("SELECT 1").execute(p).await.is_ok(),
        None    => false,
    };

    let s2_ok = match &state.source2_pool {
        Some(p) => sqlx::query("SELECT 1").execute(p).await.is_ok(),
        None    => false,
    };

    let sync = state.sync_state.read().await;

    Ok(Json(HealthResponse {
        status:       if local_ok { "ok".into() } else { "degraded".into() },
        local_db:     if local_ok { "ok".into() } else { "error".into() },
        source1_db:   if s1_ok   { "ok".into() } else { "unreachable".into() },
        source2_db:   if s2_ok   { "ok".into() } else { "unreachable".into() },
        sync_running: sync.running,
        last_sync_at: sync.last_sync_at.map(|t| t.to_string()),
        last_status:  sync.last_status.clone(),
    }))
}
