// src/sync/rules.rs

use chrono::{NaiveDateTime, Utc};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use crate::config::{Action, Config, DuplicateAction, MissingSensorAction};
use super::audit::{self, AuditEntry, IssueType};

#[derive(Debug)]
pub struct Reading {
    pub id:         Uuid,
    pub sensor_id:  Uuid,
    pub value:      Option<f64>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Insert,
    Discard,
    Flag,
}

pub struct RuleResult {
    pub verdict:    Verdict,
    pub audit:      Option<AuditEntry>,
}

/// Chequear timestamp futuro
pub fn check_future_timestamp(
    reading: &Reading,
    config: &Config,
    source: &str,
    sync_log_id: Uuid,
) -> Option<RuleResult> {
    let now = Utc::now().naive_utc();
    if reading.created_at <= now {
        return None;
    }

    let resolution = match config.rules.future_timestamp.action {
        Action::Discard => "discarded",
        Action::Flag    => "flagged",
    };

    info!(
        "[{}] {} reading {} — future_timestamp ({}) — rule: future_timestamp.{}",
        source, resolution.to_uppercase(), reading.id, reading.created_at, resolution
    );

    let audit = if config.rules.future_timestamp.log {
        Some(AuditEntry {
            sync_log_id,
            source: source.to_string(),
            issue_type: IssueType::FutureTimestamp,
            record_id: Some(reading.id),
            details: audit::details_future_timestamp(reading.id, reading.created_at),
            resolution: resolution.to_string(),
        })
    } else {
        None
    };

    Some(RuleResult {
        verdict: if config.rules.future_timestamp.action == Action::Discard {
            Verdict::Discard
        } else {
            Verdict::Flag
        },
        audit,
    })
}

/// Chequear timestamp pasado fuera del threshold
pub fn check_past_threshold(
    reading: &Reading,
    config: &Config,
    source: &str,
    sync_log_id: Uuid,
) -> Option<RuleResult> {
    let threshold = chrono::NaiveDate::parse_from_str(
        &config.rules.past_threshold.threshold, "%Y-%m-%d"
    )
    .unwrap_or(chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
    .and_hms_opt(0, 0, 0)
    .unwrap();

    if reading.created_at >= threshold {
        return None;
    }

    let resolution = match config.rules.past_threshold.action {
        Action::Discard => "discarded",
        Action::Flag    => "flagged",
    };

    info!(
        "[{}] {} reading {} — past_threshold ({}) — rule: past_threshold.{}",
        source, resolution.to_uppercase(), reading.id, reading.created_at, resolution
    );

    let audit = if config.rules.past_threshold.log {
        Some(AuditEntry {
            sync_log_id,
            source: source.to_string(),
            issue_type: IssueType::PastThreshold,
            record_id: Some(reading.id),
            details: audit::details_past_threshold(
                reading.id,
                reading.created_at,
                &config.rules.past_threshold.threshold,
            ),
            resolution: resolution.to_string(),
        })
    } else {
        None
    };

    Some(RuleResult {
        verdict: if config.rules.past_threshold.action == Action::Discard {
            Verdict::Discard
        } else {
            Verdict::Flag
        },
        audit,
    })
}

/// Chequear valor nulo
pub fn check_null_value(
    reading: &Reading,
    config: &Config,
    source: &str,
    sync_log_id: Uuid,
) -> Option<RuleResult> {
    if reading.value.is_some() {
        return None;
    }

    let resolution = match config.rules.null_value.action {
        Action::Discard => "discarded",
        Action::Flag    => "flagged",
    };

    info!(
        "[{}] {} reading {} — null_value — sensor: {} — rule: null_value.{}",
        source, resolution.to_uppercase(), reading.id, reading.sensor_id, resolution
    );

    let audit = if config.rules.null_value.log {
        Some(AuditEntry {
            sync_log_id,
            source: source.to_string(),
            issue_type: IssueType::NullValue,
            record_id: Some(reading.id),
            details: audit::details_null_value(reading.id, reading.sensor_id),
            resolution: resolution.to_string(),
        })
    } else {
        None
    };

    Some(RuleResult {
        verdict: if config.rules.null_value.action == Action::Discard {
            Verdict::Discard
        } else {
            Verdict::Flag
        },
        audit,
    })
}

/// Chequear si el sensor existe en la réplica local
pub async fn check_missing_sensor(
    reading: &Reading,
    config: &Config,
    source: &str,
    sync_log_id: Uuid,
    local_pool: &PgPool,
) -> anyhow::Result<Option<RuleResult>> {
    let exists = sqlx::query_scalar!(
        "SELECT EXISTS(SELECT 1 FROM sensors WHERE id = $1)",
        reading.sensor_id
    )
    .fetch_one(local_pool)
    .await?
    .unwrap_or(false);

    if exists {
        return Ok(None);
    }

    let resolution = match config.rules.missing_sensor.action {
        MissingSensorAction::Discard      => "discarded",
        MissingSensorAction::CreateSensor => "sensor_created",
    };

    info!(
        "[{}] {} reading {} — missing_sensor {} — rule: missing_sensor.{}",
        source, resolution.to_uppercase(), reading.id, reading.sensor_id, resolution
    );

    let audit = if config.rules.missing_sensor.log {
        Some(AuditEntry {
            sync_log_id,
            source: source.to_string(),
            issue_type: IssueType::MissingSensor,
            record_id: Some(reading.id),
            details: audit::details_missing_sensor(reading.id, reading.sensor_id),
            resolution: resolution.to_string(),
        })
    } else {
        None
    };

    Ok(Some(RuleResult {
        verdict: if config.rules.missing_sensor.action == MissingSensorAction::Discard {
            Verdict::Discard
        } else {
            Verdict::Insert // CreateSensor lo maneja el daemon antes de insertar
        },
        audit,
    }))
}

/// Chequear si el valor está fuera del rango de alertas configurado
pub async fn check_out_of_range(
    reading: &Reading,
    config: &Config,
    source: &str,
    sync_log_id: Uuid,
    local_pool: &PgPool,
) -> anyhow::Result<Option<RuleResult>> {
    let value = match reading.value {
        Some(v) => v,
        None    => return Ok(None),
    };

    let range = sqlx::query!(
        r#"
        SELECT ar.range_min::float8 AS "range_min!", ar.range_max::float8 AS "range_max!"
        FROM alert_ranges ar
        JOIN sensors s ON
            s.type          = ar.sensor_type
            AND s.sensor_number = ar.sensor_number
        JOIN boxes b ON b.id = s.box_id AND b.name = ar.box_name
        WHERE s.id = $1
        LIMIT 1
        "#,
        reading.sensor_id,
    )
    .fetch_optional(local_pool)
    .await?;

    let range = match range {
        Some(r) => r,
        None    => return Ok(None), // sin rango configurado, no se puede chequear
    };

    if value >= range.range_min && value <= range.range_max {
        return Ok(None);
    }

    let resolution = match config.rules.out_of_range.action {
        Action::Discard => "discarded",
        Action::Flag    => "flagged",
    };

    info!(
        "[{}] FLAG reading {} — out_of_range — value: {}, range: [{}, {}] — sensor: {}",
        source, reading.id, value, range.range_min, range.range_max, reading.sensor_id
    );

    let audit = if config.rules.out_of_range.log {
        Some(AuditEntry {
            sync_log_id,
            source: source.to_string(),
            issue_type: IssueType::OutOfRange,
            record_id: Some(reading.id),
            details: audit::details_out_of_range(
                reading.id,
                reading.sensor_id,
                value,
                range.range_min,
                range.range_max,
            ),
            resolution: resolution.to_string(),
        })
    } else {
        None
    };

    Ok(Some(RuleResult {
        verdict: if config.rules.out_of_range.action == Action::Discard {
            Verdict::Discard
        } else {
            Verdict::Flag
        },
        audit,
    }))
}

/// Resolver conflicto de duplicado entre dos fuentes
pub fn resolve_duplicate(
    reading_s1: &Reading,
    reading_s2: &Reading,
    config: &Config,
    sync_log_id: Uuid,
) -> (Reading, Option<AuditEntry>) {
    let value_s1 = reading_s1.value.unwrap_or(0.0);
    let value_s2 = reading_s2.value.unwrap_or(0.0);

    let winner = match config.rules.duplicate.action {
        DuplicateAction::PreferSource1 => {
            info!(
                "[conflict] PREFER source1 reading {} — duplicate — delta: {:.4} — rule: duplicate.prefer_source1",
                reading_s1.id,
                (value_s1 - value_s2).abs()
            );
            reading_s1.id
        }
        DuplicateAction::PreferSource2 => {
            info!(
                "[conflict] PREFER source2 reading {} — duplicate — delta: {:.4} — rule: duplicate.prefer_source2",
                reading_s2.id,
                (value_s1 - value_s2).abs()
            );
            reading_s2.id
        }
        DuplicateAction::Average => {
            info!(
                "[conflict] AVERAGE duplicate — sensor: {} created_at: {} — values: {:.4} / {:.4}",
                reading_s1.sensor_id, reading_s1.created_at, value_s1, value_s2
            );
            reading_s1.id // usamos el id de s1 pero con valor promediado
        }
        DuplicateAction::Discard => {
            info!(
                "[conflict] DISCARD duplicate — sensor: {} created_at: {}",
                reading_s1.sensor_id, reading_s1.created_at
            );
            reading_s1.id
        }
    };

    let resolution = format!("{:?}", config.rules.duplicate.action).to_lowercase();

    let audit = if config.rules.duplicate.log {
        Some(AuditEntry {
            sync_log_id,
            source: "conflict".to_string(),
            issue_type: IssueType::Duplicate,
            record_id: Some(winner),
            details: audit::details_duplicate(
                reading_s1.id,
                reading_s1.sensor_id,
                reading_s1.created_at,
                value_s1,
                value_s2,
            ),
            resolution,
        })
    } else {
        None
    };

    // Retornar el reading ganador (con valor promedio si aplica)
    let result = match config.rules.duplicate.action {
        DuplicateAction::Average => Reading {
            id:         reading_s1.id,
            sensor_id:  reading_s1.sensor_id,
            value:      Some((value_s1 + value_s2) / 2.0),
            created_at: reading_s1.created_at,
        },
        DuplicateAction::PreferSource2 => Reading {
            id:         reading_s2.id,
            sensor_id:  reading_s2.sensor_id,
            value:      reading_s2.value,
            created_at: reading_s2.created_at,
        },
        _ => Reading {
            id:         reading_s1.id,
            sensor_id:  reading_s1.sensor_id,
            value:      reading_s1.value,
            created_at: reading_s1.created_at,
        },
    };

    (result, audit)
}
