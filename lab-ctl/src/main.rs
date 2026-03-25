// src/main.rs

mod config;
mod db;
mod routes;
mod sync;

use std::sync::Arc;
use std::time::Duration;

use axum::{routing::{get, post, delete}, Router};
use sqlx::PgPool;
use tokio_cron_scheduler::{Job, JobScheduler};
use tower_http::{cors::{Any, CorsLayer}, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use config::Config;
use sync::daemon::{new_state, run_sync, SharedSyncState};

#[derive(Clone)]
pub struct AppState {
    pub config:        Arc<Config>,
    pub local_pool:    PgPool,
    pub source1_pool:  Option<PgPool>,
    pub source2_pool:  Option<PgPool>,
    pub sync_state:    SharedSyncState,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Cargar config
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "/config/sync-config.toml".to_string());

    let config = Arc::new(Config::load(&config_path)?);
    info!("Config cargada desde {}", config_path);
    info!("Sync interval: {} minutos", config.sync.interval_minutes);

    // Conectar a DBs
    let local_pool = db::connect_local().await?;
    db::init_schema(&local_pool).await?;

    let source1_pool = db::connect_source(&config.sources.source1, "source1")
        .await
        .map_err(|e| { tracing::warn!("source1 no disponible: {}", e); e })
        .ok();

    let source2_pool = db::connect_source(&config.sources.source2, "source2")
        .await
        .map_err(|e| { tracing::warn!("source2 no disponible: {}", e); e })
        .ok();

    let sync_state = new_state();

    let state = AppState {
        config:       config.clone(),
        local_pool:   local_pool.clone(),
        source1_pool: source1_pool.clone(),
        source2_pool: source2_pool.clone(),
        sync_state:   sync_state.clone(),
    };

    // Scheduler de sync automático
    let interval = config.sync.interval_minutes;
    {
        let config      = config.clone();
        let local_pool  = local_pool.clone();
        let s1_pool     = source1_pool.clone();
        let s2_pool     = source2_pool.clone();
        let sync_state  = sync_state.clone();

        let sched = JobScheduler::new().await?;
        let cron  = format!("0 */{} * * * *", interval);

        sched.add(Job::new_async(cron.as_str(), move |_, _| {
            let config     = config.clone();
            let local      = local_pool.clone();
            let s1         = s1_pool.clone();
            let s2         = s2_pool.clone();
            let sync_state = sync_state.clone();

            Box::pin(async move {
                run_sync(config, local, s1, s2, sync_state).await;
            })
        })?).await?;

        sched.start().await?;
        info!("Sync scheduler iniciado — cada {} minutos", interval);
    }

    // Router
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        // Health
        .route("/health", get(routes::health::health))

        // Sync
        .route("/sync/trigger",      post(routes::sync::trigger_sync))
        .route("/sync/status",       get(routes::sync::sync_status))
        .route("/sync/logs",         get(routes::sync::sync_logs))
        .route("/sync/audit",        get(routes::sync::audit_list))
        .route("/sync/audit/summary", get(routes::sync::audit_summary))

        // DB info
        .route("/db/stats",          get(routes::db_info::db_stats))
        .route("/db/sources/diff",   get(routes::db_info::db_sources_diff))
        .route("/db/anomalies",      get(routes::db_info::db_anomalies))

        // Containers
        .route("/containers",                    get(routes::containers::list_containers))
        .route("/containers/:name/start",        post(routes::containers::start_container))
        .route("/containers/:name/stop",         post(routes::containers::stop_container))
        .route("/containers/:name/restart",      post(routes::containers::restart_container))
        .route("/containers/:name/logs",         get(routes::containers::container_logs))

        // Backup
        .route("/backups",           get(routes::backup::list_backups))
        .route("/backups",           post(routes::backup::create_backup))
        .route("/backups/:filename", delete(routes::backup::delete_backup))

        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3003);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("lab-ctl escuchando en http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}