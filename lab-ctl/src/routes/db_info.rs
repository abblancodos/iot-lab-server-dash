// src/routes/db.rs

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::AppState;

#[derive(Serialize)]
pub struct DbStats {
    pub total_readings:  i64,
    pub total_sensors:   i64,
    pub total_boxes:     i64,
    pub first_reading:   Option<String>,
    pub last_reading:    Option<String>,
    pub db_size:         String,
}

pub async fn db_stats(State(state): State<AppState>) -> Result<Json<DbStats>, StatusCode> {
    let counts = sqlx::query!(
        r#"
        SELECT
            (SELECT COUNT(*) FROM readings)                                          AS "total_readings!: i64",
            (SELECT COUNT(*) FROM sensors)                                           AS "total_sensors!: i64",
            (SELECT COUNT(*) FROM boxes)                                             AS "total_boxes!: i64",
            (SELECT MIN(created_at) FROM readings WHERE created_at > '2020-01-01')  AS first_reading,
            (SELECT MAX(created_at) FROM readings WHERE created_at < now())          AS last_reading,
            pg_size_pretty(pg_database_size(current_database()))                    AS "db_size!: String"
        "#,
    )
    .fetch_one(&state.local_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DbStats {
        total_readings: counts.total_readings,
        total_sensors:  counts.total_sensors,
        total_boxes:    counts.total_boxes,
        first_reading:  counts.first_reading.map(|t| t.to_string()),
        last_reading:   counts.last_reading.map(|t| t.to_string()),
        db_size:        counts.db_size,
    }))
}

#[derive(Serialize)]
pub struct SourceDiff {
    pub source:         String,
    pub reachable:      bool,
    pub total_readings: Option<i64>,
    pub last_reading:   Option<String>,
    pub local_total:    i64,
    pub diff:           Option<i64>,
}

pub async fn db_sources_diff(State(state): State<AppState>) -> Json<Vec<SourceDiff>> {
    let local_total: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM readings")
        .fetch_one(&state.local_pool)
        .await
        .unwrap_or(Some(0))
        .unwrap_or(0);

    let mut diffs = vec![];

    for (name, pool) in [
        ("source1", &state.source1_pool),
        ("source2", &state.source2_pool),
    ] {
        match pool {
            Some(p) => {
                let result = sqlx::query!(
                    r#"
                    SELECT
                        COUNT(*)         AS "total!: i64",
                        MAX(created_at)  AS last_reading
                    FROM readings
                    WHERE created_at < now()
                    "#,
                )
                .fetch_one(p)
                .await;

                match result {
                    Ok(r) => diffs.push(SourceDiff {
                        source:         name.to_string(),
                        reachable:      true,
                        total_readings: Some(r.total),
                        last_reading:   r.last_reading.map(|t| t.to_string()),
                        local_total,
                        diff:           Some(r.total - local_total),
                    }),
                    Err(_) => diffs.push(SourceDiff {
                        source:         name.to_string(),
                        reachable:      false,
                        total_readings: None,
                        last_reading:   None,
                        local_total,
                        diff:           None,
                    }),
                }
            }
            None => diffs.push(SourceDiff {
                source:         name.to_string(),
                reachable:      false,
                total_readings: None,
                last_reading:   None,
                local_total,
                diff:           None,
            }),
        }
    }

    Json(diffs)
}

#[derive(Serialize)]
pub struct Anomaly {
    pub id:         uuid::Uuid,
    pub sensor_id:  Option<uuid::Uuid>,
    pub value:      Option<f64>,
    pub created_at: String,
    pub reason:     String,
}

pub async fn db_anomalies(State(state): State<AppState>) -> Result<Json<Vec<Anomaly>>, StatusCode> {
    let now = chrono::Utc::now().naive_utc();

    let rows = sqlx::query!(
        r#"
        SELECT
            id,
            sensor_id,
            value::float8 AS value,
            created_at,
            CASE
                WHEN created_at > now()          THEN 'future_timestamp'
                WHEN created_at < '2020-01-01'   THEN 'past_threshold'
                WHEN value IS NULL               THEN 'null_value'
                ELSE 'unknown'
            END AS "reason!: String"
        FROM readings
        WHERE
            created_at > now()
            OR created_at < '2020-01-01'
            OR value IS NULL
        ORDER BY created_at DESC
        LIMIT 200
        "#,
    )
    .fetch_all(&state.local_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let _ = now;

    Ok(Json(rows.into_iter().map(|r| Anomaly {
        id:         r.id,
        sensor_id:  r.sensor_id,
        value:      r.value,
        created_at: r.created_at.to_string(),
        reason:     r.reason,
    }).collect()))
}
