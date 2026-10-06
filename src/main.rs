use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db, DbConfig, DbTarget};
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
    let db_cfg =
        DbConfig::from_env(|key| std::env::var(key).ok()).context("database configuration")?;
    info!(
        "starting nvnmchain Explorer (rpc={}, db={})",
        cfg.rpc_url,
        match &db_cfg.target {
            DbTarget::Sqlite(path) => path.clone(),
        }
    );

    let (status_tx, status_rx) = watch::channel(db::Status::starting(db_cfg.role));
    let db: Db = db::open_with(&db_cfg, status_tx)
        .await
        .context("initialize database")?;
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
    handle_signals(shutdown_tx)?;
    let indexer_shutdown = shutdown_rx.clone();
    // Home-page stats live here; the stats task refreshes them, the web
    // handlers read them. Seeded from kv so a restart paints real numbers
    // before the first recompute.
    let home_stats = Arc::new(std::sync::RwLock::new(
        db::get_kv(&db, "stats")
            .await
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
    let app = web::health(status_rx, shutdown_rx.clone()).merge(web::app(state));

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

    let mut stopping = shutdown_rx.clone();
    tokio::select! {
        // A signal stops the server too, so check for one first.
        biased;
        _ = stopping.wait_for(|&stop| stop) => {}
        r = &mut server => {
            r.context("server task")?.context("server error")?;
            anyhow::bail!("server stopped without a shutdown signal");
        }
    }

    // In-flight connections and the indexer loops drain together, within the
    // deadline the signal thread holds. A batch cut short there is replayed on
    // the next start. Dropping the runtime instead would wait on blocking
    // tasks with no bound at all.
    match server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!("server error while shutting down: {e}"),
        Err(e) => error!("server task failed while shutting down: {e}"),
    }
    if let Err(e) = indexer_task.await {
        error!("indexer task failed while shutting down: {e}");
    }
    info!("shutdown complete");
    std::process::exit(0)
}

/// How long shutdown may take, from the signal to the process exiting.
const SHUTDOWN_SECS: u64 = 3;

/// Handle SIGINT and SIGTERM on a thread of their own, never on the runtime:
/// inline SQLite calls can hold every worker, and a runtime timer or signal
/// listener waits for a free one. The first signal flips `shutdown` and starts
/// the `SHUTDOWN_SECS` deadline on another thread; a second one exits at once.
fn handle_signals(shutdown: watch::Sender<bool>) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use signal_hook::consts::{SIGINT, SIGTERM};
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])
            .context("install the signal handlers")?;
        std::thread::Builder::new()
            .name("signals".into())
            .spawn(move || {
                let mut names =
                    signals
                        .forever()
                        .map(|sig| if sig == SIGINT { "SIGINT" } else { "SIGTERM" });
                if let Some(sig) = names.next() {
                    begin_shutdown(sig, &shutdown);
                }
                if let Some(sig) = names.next() {
                    force_quit(sig);
                }
            })
            .context("start the signal thread")?;
    }
    // Elsewhere (Windows) Ctrl+C is all there is, and it is noticed on the
    // runtime; the deadline still runs on a thread of its own.
    #[cfg(not(unix))]
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        begin_shutdown("SIGINT", &shutdown);
        let _ = tokio::signal::ctrl_c().await;
        force_quit("SIGINT");
    });
    Ok(())
}

fn begin_shutdown(sig: &'static str, shutdown: &watch::Sender<bool>) {
    // The deadline first, so nothing below can hold it up.
    let deadline = std::thread::Builder::new()
        .name("deadline".into())
        .spawn(move || {
            std::thread::sleep(Duration::from_secs(SHUTDOWN_SECS));
            warn!("still busy {SHUTDOWN_SECS}s after {sig}; exiting anyway");
            std::process::exit(0);
        });
    shutdown.send_replace(true);
    info!("received {sig}, shutting down (second one force quits)");
    if let Err(e) = deadline {
        warn!("no {SHUTDOWN_SECS}s deadline, so the drain may run long: {e}");
    }
}

fn force_quit(sig: &str) -> ! {
    warn!("received {sig} again, exiting now");
    std::process::exit(130);
}
