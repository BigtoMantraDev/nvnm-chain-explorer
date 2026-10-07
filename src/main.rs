use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::indexer::{self, IndexerConfig};
use nvnmchain_explorer::rpc::ChainRpc;
use nvnmchain_explorer::web;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nvnmchain_explorer=info".into()),
        )
        .init();

    let cfg = Settings::from_env();
    info!(
        "starting nvnmchain Explorer (rpc={}, db={})",
        cfg.rpc_url, cfg.db_path
    );

    let db: Db = db::open(&cfg.db_path).context("initialize database")?;
    let rpc = ChainRpc::from_settings(&cfg)?;
    let tera = web::build_tera(db.clone())?;

    // Background indexer: instant heads via WebSocket (poll fallback),
    // concurrent block fetching, serialized SQLite writes.
    let indexer_rpc = ChainRpc::from_settings(&cfg)?;
    let indexer_db = db.clone();
    let indexer_cfg = IndexerConfig::from_settings(&cfg);
    let ws_url = cfg.ws_url.clone();
    // Sized for ~an hour of sub-second blocks; combined with the writer's
    // in-order emission and the SSE lag-replay, live viewers never see gaps.
    let (block_tx, _) = broadcast::channel::<serde_json::Value>(8192);
    let indexer_block_tx = block_tx.clone();
    // Ctrl+C (or SIGTERM) flips this watch; every indexer loop checks it so
    // the process stops promptly instead of continuing to fetch and index.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let indexer_shutdown = shutdown_rx.clone();
    // Home-page stats live here; the stats task refreshes them, the web
    // handlers read them. Seeded from kv so a restart paints real numbers
    // before the first recompute.
    let home_stats = Arc::new(std::sync::RwLock::new(
        db::get_kv(&db, "stats")
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::Value::Null),
    ));
    let indexer_stats = home_stats.clone();
    let indexer_task = tokio::spawn(async move {
        info!("indexer websocket feed: {ws_url}");
        indexer::run_forever(
            indexer_rpc,
            indexer_db,
            indexer_cfg,
            indexer_block_tx,
            indexer_stats,
            indexer_shutdown,
        )
        .await
    });

    let state = web::AppState {
        db,
        rpc,
        cfg: cfg.clone(),
        tera,
        block_events: block_tx,
        stats: home_stats,
        shutdown: shutdown_rx.clone(),
    };
    let app = web::app(state);

    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("listening on http://{addr}");
    let mut stop = shutdown_rx.clone();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = stop.wait_for(|&stop| stop).await;
    });
    let mut server = tokio::spawn(async move { server.await });

    let sig = tokio::select! {
        r = &mut server => {
            r.context("server task")?.context("server error")?;
            anyhow::bail!("server stopped without a shutdown signal");
        }
        sig = shutdown_signal() => sig,
    };
    info!("received {sig}, shutting down (second one force quits)");
    let _ = shutdown_tx.send(true);
    // The installed handler replaced the default disposition for good, so
    // signals no longer kill the process; catch a second one ourselves.
    tokio::spawn(async {
        let sig = shutdown_signal().await;
        warn!("received {sig} again, exiting now");
        std::process::exit(130);
    });

    // One budget for in-flight connections and the indexer loops together.
    // A batch cut short here is replayed on the next start. Dropping the
    // runtime instead would wait on blocking tasks with no bound at all.
    let drained = tokio::time::timeout(Duration::from_secs(SHUTDOWN_SECS), async {
        match server.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("server error while shutting down: {e}"),
            Err(e) => error!("server task failed while shutting down: {e}"),
        }
        if let Err(e) = indexer_task.await {
            error!("indexer task failed while shutting down: {e}");
        }
    })
    .await;
    if drained.is_err() {
        warn!("still busy {SHUTDOWN_SECS}s after {sig}; exiting anyway");
    }
    info!("shutdown complete");
    std::process::exit(0)
}

/// How long shutdown may take, from the signal to the process exiting.
const SHUTDOWN_SECS: u64 = 3;

/// Wait for SIGINT or SIGTERM, returning which arrived.
async fn shutdown_signal() -> &'static str {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => "SIGINT",
        _ = terminate => "SIGTERM",
    }
}
