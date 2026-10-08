//! Fixture replay: the baseline fixtures' blocks, rebuilt as the bundles the
//! indexer writes, go into both backends, and every bundle-derived table must
//! equal the fixture's.
//!
//! Network-free, but it needs a Postgres server: ignored unless run with
//! `--include-ignored`, and then failing without `PG_TEST_URL`.

use std::path::{Path, PathBuf};

use nvnmchain_explorer::db::{self, Db, Role, Status};
use rusqlite::{Connection, OpenFlags};
use sqlx::{AssertSqlSafe, Connection as _, PgConnection};

#[path = "common/backend.rs"]
mod backend;
#[allow(dead_code)]
mod common;
use common::baseline::{columns, diff_rows, pg_rows, rows, SPECS};
#[path = "common/bundles.rs"]
mod bundles;
use bundles::{bundles, genesis};

fn fixtures() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/baseline");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "db"))
        .collect();
    found.sort();
    found
}

fn open_fixture(path: &Path) -> Connection {
    let abs = std::fs::canonicalize(path).unwrap();
    Connection::open_with_flags(
        format!("file:{}?immutable=1", abs.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap()
}

enum Target {
    Sqlite {
        _dir: tempfile::TempDir,
        path: PathBuf,
        db: Db,
    },
    Postgres {
        _scratch: backend::Scratch,
        url: String,
        db: Db,
    },
}

impl Target {
    async fn sqlite(name: &str) -> Target {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        let db = db::open(path.to_str().unwrap()).await.unwrap();
        Target::Sqlite {
            _dir: dir,
            path,
            db,
        }
    }

    async fn postgres() -> Target {
        let (scratch, url) = backend::scratch_schema().await;
        let cfg = backend::pg_config(&url, Role::All);
        let db = db::open_with(
            &cfg,
            tokio::sync::watch::channel(Status::starting(Role::All)).0,
        )
        .await
        .unwrap();
        Target::Postgres {
            _scratch: scratch,
            url,
            db,
        }
    }

    fn db(&self) -> &Db {
        match self {
            Target::Sqlite { db, .. } | Target::Postgres { db, .. } => db,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Target::Sqlite { .. } => "SQLite",
            Target::Postgres { .. } => "Postgres",
        }
    }

    async fn execute(&self, sql: &str) {
        match self {
            Target::Sqlite { path, .. } => {
                Connection::open(path).unwrap().execute_batch(sql).unwrap();
            }
            Target::Postgres { url, .. } => {
                let mut conn = PgConnection::connect(url).await.unwrap();
                sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
                    .execute(&mut conn)
                    .await
                    .unwrap();
            }
        }
    }

    /// Every compared table's differences from `fixture`.
    async fn diff(&self, fixture: &Connection, only: Option<&[&str]>) -> Vec<String> {
        let mut diffs = Vec::new();
        for spec in SPECS {
            if only.is_some_and(|o| !o.contains(&spec.table)) {
                continue;
            }
            let cols: Vec<String> = columns(fixture, spec.table)
                .into_iter()
                .filter(|c| !spec.skip.contains(&c.as_str()))
                .collect();
            let base = rows(fixture, spec, &cols);
            let new = match self {
                Target::Sqlite { path, .. } => rows(&Connection::open(path).unwrap(), spec, &cols),
                Target::Postgres { url, .. } => {
                    let mut conn = PgConnection::connect(url).await.unwrap();
                    pg_rows(&mut conn, spec, &cols).await
                }
            };
            diffs.extend(diff_rows(spec.table, &base, &new, ("fixture", self.name())));
        }
        diffs
    }
}

/// Write the fixture's bundles in chunks of 64, then its genesis balances, and
/// repair. Returns the most statements one Postgres batch took.
async fn replay(target: &Target, fixture: &Connection) -> u64 {
    let db = target.db();
    let mut worst = 0;
    for chunk in bundles(fixture).chunks(64) {
        let before = db::statements();
        db::save_block_bundles(db, chunk).await.unwrap();
        worst = worst.max(db::statements() - before);
    }
    let (balances, cursor) = genesis(fixture);
    db::save_genesis_balances(db, &balances, cursor)
        .await
        .unwrap();
    db::repair_derived_tables(db).await;
    worst
}

fn report(what: &str, diffs: &[String]) {
    for d in diffs.iter().take(25) {
        eprintln!("  {d}");
    }
    assert!(diffs.is_empty(), "{what}: {} difference(s)", diffs.len());
}

/// One test, so nothing else in this binary sends statements while the
/// round trips are counted.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn every_fixture_replays_into_both_backends() {
    for path in fixtures() {
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        let fixture = open_fixture(&path);
        for target in [Target::sqlite(&name).await, Target::postgres().await] {
            let worst = replay(&target, &fixture).await;
            if matches!(target, Target::Postgres { .. }) {
                assert!(
                    worst <= 14,
                    "{name}: a batch took {worst} round trips; the budget is 14"
                );
            }
            report(
                &format!("{name} into {}", target.name()),
                &target.diff(&fixture, None).await,
            );

            // Rebuilt from the history alone, balances and holder counts must
            // come out the same, keys spelled as the incremental path spells them.
            target.execute("DELETE FROM token_balances").await;
            db::repair_derived_tables(target.db()).await;
            report(
                &format!("{name} rebuilt on {}", target.name()),
                &target
                    .diff(&fixture, Some(&["token_balances", "token_metadata"]))
                    .await,
            );
        }
    }
}
