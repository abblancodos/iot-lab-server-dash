// src/routes/backup.rs

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use serde::Serialize;
use std::path::PathBuf;
use tokio::process::Command;

use crate::AppState;

const BACKUP_DIR: &str = "/backups";

#[derive(Serialize)]
pub struct BackupEntry {
    pub filename:   String,
    pub size_bytes: u64,
    pub created_at: String,
}

pub async fn list_backups(State(_state): State<AppState>) -> Result<Json<Vec<BackupEntry>>, StatusCode> {
    let mut entries = tokio::fs::read_dir(BACKUP_DIR)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut backups = vec![];

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("dump") {
            continue;
        }
        let meta = tokio::fs::metadata(&path).await.unwrap();
        let filename = path.file_name().unwrap().to_string_lossy().to_string();
        let created_at = meta.modified().unwrap();
        let created_at: chrono::DateTime<Utc> = created_at.into();

        backups.push(BackupEntry {
            filename,
            size_bytes: meta.len(),
            created_at: created_at.to_rfc3339(),
        });
    }

    backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(Json(backups))
}

pub async fn create_backup(State(state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let filename  = format!("agrodash_{}.dump", timestamp);
    let path      = PathBuf::from(BACKUP_DIR).join(&filename);

    let db_url = std::env::var("DATABASE_URL").unwrap_or_default();

    let output = Command::new("pg_dump")
        .arg("--dbname").arg(&db_url)
        .arg("-Fc")
        .arg("-f").arg(path.to_str().unwrap())
        .output()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Ok(Json(serde_json::json!({
            "status": "error",
            "error":  err.to_string()
        })));
    }

    // Mantener solo los últimos 7 backups
    cleanup_old_backups(7).await;

    Ok(Json(serde_json::json!({
        "status":   "success",
        "filename": filename,
    })))
}

pub async fn delete_backup(
    State(_state): State<AppState>,
    Path(filename): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Sanitizar filename para evitar path traversal
    if filename.contains('/') || filename.contains("..") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let path = PathBuf::from(BACKUP_DIR).join(&filename);

    tokio::fs::remove_file(&path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(Json(serde_json::json!({ "status": "deleted", "filename": filename })))
}

async fn cleanup_old_backups(keep: usize) {
    let Ok(mut entries) = tokio::fs::read_dir(BACKUP_DIR).await else { return };

    let mut files = vec![];
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("dump") {
            files.push(path);
        }
    }

    files.sort();
    if files.len() > keep {
        for old in &files[..files.len() - keep] {
            let _ = tokio::fs::remove_file(old).await;
        }
    }
}