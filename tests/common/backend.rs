#![allow(dead_code)]
//! The database a test runs against.
//!
//! Included by path (`#[path = "common/backend.rs"] mod backend;`) so a suite
//! that needs only this does not compile the baseline helpers too.

use nvnmchain_explorer::db::{self, Db};

/// Keep this alive as long as the database is used.
pub enum TempDb {
    Sqlite(tempfile::TempDir),
}

/// A fresh database for one test. Keep the guard alive for as long as the
/// database is used; dropping it removes the database.
pub async fn temp_db(name: &str) -> (TempDb, Db) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    (
        TempDb::Sqlite(dir),
        db::open(path.to_str().unwrap()).await.expect("open db"),
    )
}
