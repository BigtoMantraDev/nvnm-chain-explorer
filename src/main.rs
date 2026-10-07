use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tower::ServiceExt;
use tracing::{error, info, warn};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db, DbConfig, DbTarget, Role};
use nvnmchain_explorer::follow::{self, Follower};
use nvnmchain_explorer::indexer::{self, IndexerConfig};
use nvnmchain_explorer::rpc::ChainRpc;
use nvnmchain_explorer::{metrics, web};

/// The settings file goes into the environment before anything reads it,
/// and before the runtime starts its threads: setting a variable races with
/// another thread reading one.
fn main() -> anyhow::Result<()> {
    let env_file = read_env_file()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("start the runtime")?
        .block_on(run(env_file))
}

/// Load `ENV_FILE`, or `.env` in the working directory when that is unset,
/// leaving every variable already set as it is; within the file, a later line
/// wins. An empty `ENV_FILE` reads nothing, and no `.env` file is no error; a
/// missing named file or a malformed one is. Returns the file read.
fn read_env_file() -> anyhow::Result<Option<PathBuf>> {
    let (path, named) = match std::env::var_os("ENV_FILE") {
        Some(path) if path.is_empty() => return Ok(None),
        Some(path) => (PathBuf::from(path), true),
        None => (PathBuf::from(".env"), false),
    };
    // Not a directory of that name, say a virtualenv, nor a file in a
    // directory this user cannot look into.
    if !named && !path.is_file() {
        return Ok(None);
    }
    let read = || format!("read {}", path.display());
    let mut settings: Vec<(String, String)> = Vec::new();
    for item in dotenvy::from_path_iter(&path).with_context(read)? {
        match item {
            Ok(setting) => settings.push(setting),
            // The parser's message quotes the line, and for an unclosed quote
            // the rest of the file: a secret, as likely as not.
            Err(dotenvy::Error::LineParse(..)) => {
                let after = match settings.last() {
                    Some((key, _)) => format!("after {key}"),
                    None => "before the first setting".into(),
                };
                anyhow::bail!(
                    "read {}: a line {after} does not parse (not shown: it may hold a secret)",
                    path.display()
                );
            }
            Err(e) => return Err(e).with_context(read),
        }
    }
    let mut seen = HashSet::new();
    for (key, value) in settings.into_iter().rev() {
        if seen.insert(key.clone()) && std::env::var_os(&key).is_none() {
            std::env::set_var(key, value);
        }
    }
    Ok(Some(path))
}

async fn run(env_file: Option<PathBuf>) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nvnmchain_explorer=info".into()),
        )
        .init();
    if let Some(path) = env_file {
        info!(
            "read settings from {}; a variable already set wins over it",
            path.display()
        );
    }

    let cfg = Settings::from_env();
    let db_cfg =
        DbConfig::from_env(|key| std::env::var(key).ok()).context("database configuration")?;
    let role = db_cfg.role;
    info!(
        "starting nvnmchain Explorer (role={role}, rpc={}, db={})",
        cfg.rpc_url,
        match &db_cfg.target {
            DbTarget::Sqlite(path) => path.clone(),
            // `DbUrl` never shows a password.
            DbTarget::Postgres(url) => url.to_string(),
        }
    );

    let (status_tx, status_rx) = watch::channel(db::Status::starting(role));
    // Ctrl+C (or SIGTERM) flips this watch; every loop checks it so the
    // process stops promptly instead of continuing to fetch and index.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    handle_signals(shutdown_tx)?;

    // Bind first: the probes answer while the database opens, which for an
    // indexer can mean waiting as a candidate for the writer's lock.
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("listening on http://{addr}");
    let pages: Arc<OnceLock<Router>> = Arc::new(OnceLock::new());
    let mut app = web::health(status_rx, shutdown_rx.clone());
    if role != Role::All {
        // In-cluster scraping only: `ROLE=all` runs where there is no Ingress
        // to keep /metrics private.
        app = app.merge(metrics::install()?);
    }
    if role != Role::Indexer {
        let pages = pages.clone();
        app = app.fallback(move |req: Request| {
            let pages = pages.clone();
            async move {
                match pages.get() {
                    Some(router) => router.clone().oneshot(req).await.into_response(),
                    None => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [(header::RETRY_AFTER, "5")],
                        "starting",
                    )
                        .into_response(),
                }
            }
        });
    }
    let mut stop = shutdown_rx.clone();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = stop.wait_for(|&stop| stop).await;
    });
    let mut server = tokio::spawn(async move { server.await });

    let mut stopping = shutdown_rx.clone();
    let db: Db = tokio::select! {
        opened = db::open_with(&db_cfg, status_tx) => match opened {
            Ok(db) => db,
            Err(e) => {
                // A preflight refusal or a configuration error: a broken image
                // must never replace a working writer, so it exits, unready.
                error!("database: {e:#}");
                std::process::exit(1);
            }
        },
        _ = stopping.wait_for(|&stop| stop) => {
            info!("stopped before the database opened");
            std::process::exit(0);
        }
    };

    // Sized for ~an hour of sub-second blocks; combined with the writer's
    // in-order emission and the SSE lag-replay, live viewers never see gaps.
    let (block_tx, _) = broadcast::channel::<serde_json::Value>(8192);
    // Home-page stats live here; the stats task (or, on a web replica, the
    // follower) refreshes them, the web handlers read them. Seeded from kv so
    // a restart paints real numbers before the first recompute.
    let home_stats = Arc::new(std::sync::RwLock::new(
        db::get_kv(&db, "stats")
            .await
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::Value::Null),
    ));

    let mut core = None;
    match role {
        Role::All | Role::Indexer => {
            let indexer_rpc = ChainRpc::from_settings(&cfg)?;
            if role == Role::Indexer {
                tokio::spawn(metrics::watch_status(db.clone()));
            }
            let (db, events, stats, stop) = (
                db.clone(),
                block_tx.clone(),
                home_stats.clone(),
                shutdown_rx.clone(),
            );
            let indexer_cfg = IndexerConfig::from_settings(&cfg);
            info!("indexer websocket feed: {}", cfg.ws_url);
            core = Some(tokio::spawn(async move {
                indexer::run_forever(indexer_rpc, db, indexer_cfg, events, stats, stop).await
            }));
        }
        Role::Web => {
            let follower = Follower::new(db.clone(), block_tx.clone(), home_stats.clone());
            tokio::spawn(follow::run(
                follower,
                db_cfg.follow_poll,
                shutdown_rx.clone(),
            ));
        }
    }
    if role != Role::Indexer {
        let state = web::AppState {
            tera: web::build_tera(db.clone())?,
            rpc: ChainRpc::from_settings(&cfg)?,
            db,
            cfg: cfg.clone(),
            block_events: block_tx,
            stats: home_stats,
            shutdown: shutdown_rx.clone(),
        };
        let _ = pages.set(web::app(state));
    }

    let ended = async {
        match &mut core {
            Some(task) => task.await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        // A signal stops the server and the indexer too, so check for one first.
        biased;
        _ = stopping.wait_for(|&stop| stop) => {}
        r = &mut server => {
            r.context("server task")?.context("server error")?;
            anyhow::bail!("server stopped without a shutdown signal");
        }
        r = ended => {
            match r {
                Ok(Err(e)) => error!("{e}; exiting so the process restarts"),
                Ok(Ok(())) => error!("the indexer stopped; exiting so the process restarts"),
                Err(e) => error!("the indexer panicked: {e}; exiting so the process restarts"),
            }
            std::process::exit(5);
        }
    }

    // In-flight connections and the indexer loops drain together, within the
    // deadline the signal thread holds. A batch cut short there is replayed on
    // the next start; the dropped writer session frees the lock. Dropping the
    // runtime instead would wait on blocking tasks with no bound at all.
    match server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!("server error while shutting down: {e}"),
        Err(e) => error!("server task failed while shutting down: {e}"),
    }
    if let Some(core) = core {
        match core.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("{e} while shutting down"),
            Err(e) => error!("indexer task failed while shutting down: {e}"),
        }
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
