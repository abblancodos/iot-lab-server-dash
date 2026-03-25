// src/config.rs

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub sync:    SyncConfig,
    pub rules:   RulesConfig,
    pub sources: SourcesConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncConfig {
    pub interval_minutes: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RulesConfig {
    pub future_timestamp: RuleAction,
    pub past_threshold:   PastThresholdRule,
    pub duplicate:        DuplicateRule,
    pub null_value:       RuleAction,
    pub missing_sensor:   MissingSensorRule,
    pub out_of_range:     RuleAction,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuleAction {
    pub action: Action,
    pub log:    bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PastThresholdRule {
    pub threshold: String,   // e.g. "2020-01-01"
    pub action:    Action,
    pub log:       bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DuplicateRule {
    pub action: DuplicateAction,
    pub log:    bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MissingSensorRule {
    pub action: MissingSensorAction,
    pub log:    bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Discard,
    Flag,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateAction {
    PreferSource1,
    PreferSource2,
    Average,
    Discard,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MissingSensorAction {
    Discard,
    CreateSensor,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourcesConfig {
    pub source1: SourceConfig,
    pub source2: SourceConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    pub host:     String,
    pub port:     u16,
    pub db:       String,
    pub user:     String,
    pub pass:     String,
    pub priority: u8,
}

impl SourceConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.pass, self.host, self.port, self.db
        )
    }
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(Path::new(path))
            .map_err(|e| anyhow::anyhow!("No se pudo leer {}: {}", path, e))?;

        let config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Error parseando config: {}", e))?;

        Ok(config)
    }
}
