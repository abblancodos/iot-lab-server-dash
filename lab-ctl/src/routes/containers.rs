// src/routes/containers.rs

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use bollard::container::{ListContainersOptions, LogsOptions, StartContainerOptions, StopContainerOptions};
use bollard::Docker;
use futures_util::stream::StreamExt;
use serde::Serialize;
use std::collections::HashMap;

use crate::AppState;

#[derive(Serialize)]
pub struct ContainerInfo {
    pub id:     String,
    pub name:   String,
    pub image:  String,
    pub status: String,
    pub state:  String,
    pub ports:  Vec<String>,
}

pub async fn list_containers(State(_state): State<AppState>) -> Result<Json<Vec<ContainerInfo>>, StatusCode> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut filters = HashMap::new();
    filters.insert("label", vec!["com.docker.compose.project"]);

    let containers = docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        }))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = containers.into_iter().map(|c| {
        let name = c.names
            .unwrap_or_default()
            .first()
            .cloned()
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();

        let ports = c.ports.unwrap_or_default().into_iter()
            .filter_map(|p| {
                let public = p.public_port?;
                Some(format!("{}:{}", public, p.private_port))
            })
            .collect();

        ContainerInfo {
            id:     c.id.unwrap_or_default()[..12].to_string(),
            name,
            image:  c.image.unwrap_or_default(),
            status: c.status.unwrap_or_default(),
            state:  c.state.unwrap_or_default(),
            ports,
        }
    }).collect();

    Ok(Json(result))
}

pub async fn start_container(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    docker
        .start_container(&name, None::<StartContainerOptions<String>>)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "status": "started", "container": name })))
}

pub async fn stop_container(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    docker
        .stop_container(&name, Some(StopContainerOptions { t: 10 }))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "status": "stopped", "container": name })))
}

pub async fn restart_container(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    docker
        .restart_container(&name, None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "status": "restarted", "container": name })))
}

pub async fn container_logs(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let options = LogsOptions::<String> {
        stdout: true,
        stderr: true,
        tail:   "100".to_string(),
        ..Default::default()
    };

    let mut stream = docker.logs(&name, Some(options));
    let mut lines: Vec<String> = Vec::new();

    while let Some(Ok(msg)) = stream.next().await {
        lines.push(msg.to_string());
    }

    Ok(Json(serde_json::json!({ "container": name, "logs": lines })))
}