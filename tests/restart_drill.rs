//! The restart drill: Postgres restarts mid-replay, and the final tables still
//! equal the fixture. No block is lost; the writer waits, re-acquires and
//! retries.
//!
//! It restarts the server every other test uses, so it lives alone and runs
//! only when `PG_CONTAINER` names the container (`docker compose ps`), e.g.
//! `PG_CONTAINER=nvnmchain-explorer-postgres-1`.

use std::path::Path;
use std::time::Duration;

use nvnmchain_explorer::db::{self, Role, Status};
use rusqlite::{Connection, OpenFlags};
use sqlx::{Connection as _, PgConnection};

#[path = "common/backend.rs"]
mod backend;
#[path = "common/bundles.rs"]
mod bundles;
#[allow(dead_code)]
mod common;
use common::baseline::{columns, diff_rows, pg_rows, rows, SPECS};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "restarts the Postgres container; set PG_CONTAINER and run alone"]
async fn a_database_restart_mid_replay_loses_nothing() {
    let container = std::env::var("PG_CONTAINER").expect("PG_CONTAINER");
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/baseline/canary-rich.db");
    let fixture = Connection::open_with_flags(
        format!(
            "file:{}?immutable=1",
            std::fs::canonicalize(path).unwrap().display()
        ),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    let (_scratch, url) = backend::scratch_schema().await;
    let mut cfg = backend::pg_config(&url, Role::Indexer);
    cfg.tuning.lost_after = Duration::from_secs(120);
    let db = db::open_with(
        &cfg,
        tokio::sync::watch::channel(Status::starting(Role::Indexer)).0,
    )
    .await
    .unwrap();

    let chunks: Vec<Vec<_>> = bundles::bundles(&fixture)
        .chunks(16)
        .map(<[_]>::to_vec)
        .collect();
    let restart_at = chunks.len() / 3;
    for (i, chunk) in chunks.iter().enumerate() {
        if i == restart_at {
            let status = std::process::Command::new("docker")
                .args(["restart", "-t", "1", &container])
                .status()
                .expect("docker restart");
            assert!(status.success());
        }
        db::save_block_bundles(&db, chunk).await.unwrap();
    }
    let (genesis, cursor) = bundles::genesis(&fixture);
    db::save_genesis_balances(&db, &genesis, cursor)
        .await
        .unwrap();

    let mut conn = PgConnection::connect(&url).await.unwrap();
    let mut diffs = Vec::new();
    for spec in SPECS {
        let cols: Vec<String> = columns(&fixture, spec.table)
            .into_iter()
            .filter(|c| !spec.skip.contains(&c.as_str()))
            .collect();
        let (base, new) = (
            rows(&fixture, spec, &cols),
            pg_rows(&mut conn, spec, &cols).await,
        );
        diffs.extend(diff_rows(spec.table, &base, &new, ("fixture", "Postgres")));
    }
    for d in diffs.iter().take(25) {
        eprintln!("  {d}");
    }
    assert!(
        diffs.is_empty(),
        "{} difference(s) after the restart",
        diffs.len()
    );
}
