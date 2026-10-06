//! The SQLite runner: version 1 is the frozen `init_db`, and later versions
//! are the `migrations/sqlite/NNNN_*.sql` files.

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
#[cfg(test)]
use sha3::{Digest, Sha3_256};

use crate::db::migrations::{binary_version, checksum, MIGRATIONS};

/// The shape of `init_db(":memory:")`: what version 1 is on SQLite.
pub(crate) const BASELINE_SHA3: &str =
    "21ff43b56fd29b1b7f7ea0e973755d2e8fb5f751cfede043ab5c852817c91ef2";

#[cfg(test)]
/// `init_db`'s body, pragma statements aside.
const INIT_DB_BODY_SHA3: &str = "de7fb9fc801d6fd1a2f9415e97a30456de099c9511ff378857327eac7352b34d";

/// Who applied a version, for the `applied_by` column.
const APPLIED_BY: &str = concat!("nvnmchain-explorer ", env!("CARGO_PKG_VERSION"));

/// Bring a file `init_db` has just opened up to this binary's version.
///
/// One `BEGIN IMMEDIATE` transaction covers the stamp, the checks, the
/// pending versions and the shape check, so a file is stamped or migrated
/// only when the end result verifies. `init_db` stays outside it: it opens
/// its own connection, and SQLite ignores `journal_mode=WAL` inside a
/// transaction on a fresh file.
pub(super) fn run(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin the schema transaction")?;
    match migrate(conn) {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("commit the schema transaction"),
        Err(e) => {
            if let Err(rollback) = conn.execute_batch("ROLLBACK") {
                tracing::warn!("rolling back the schema transaction: {rollback}");
            }
            Err(e)
        }
    }
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
            applied_at INTEGER NOT NULL, applied_by TEXT NOT NULL)",
    )?;
    let stamped: Option<i64> = conn
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = 1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if stamped.is_none() {
        // A fresh file, or one written before versions existed: init_db has
        // just brought it to version 1.
        record(conn, 1, "baseline", BASELINE_SHA3)?;
    }
    let applied: Vec<(i64, String)> = conn
        .prepare("SELECT version, checksum FROM schema_migrations ORDER BY version")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let binary = binary_version();
    let db = applied.last().map_or(0, |(v, _)| *v);
    if db > binary {
        bail!(
            "the database is at schema version {db}, newer than this binary's {binary}; \
             run a release at version {db} or later (the only way back is forward)"
        );
    }
    for (version, stored) in &applied {
        if *stored != expected_checksum(*version) {
            bail!("migration {version} was edited after it was applied");
        }
    }
    for m in &MIGRATIONS[db as usize..] {
        let sql = m.sqlite.expect("every version after 1 has a SQLite file");
        conn.execute_batch(sql)
            .with_context(|| format!("apply migration {:04}_{}", m.version, m.name))?;
        record(conn, m.version, m.name, &checksum(sql))?;
    }
    super::schema_check::verify(conn)
}

fn expected_checksum(version: i64) -> String {
    match MIGRATIONS.get(version as usize - 1).and_then(|m| m.sqlite) {
        Some(sql) => checksum(sql),
        None => BASELINE_SHA3.to_string(),
    }
}

fn record(conn: &Connection, version: i64, name: &str, sum: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO schema_migrations (version, name, checksum, applied_at, applied_by)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![version, name, sum, super::now_ts(), APPLIED_BY],
    )?;
    Ok(())
}

#[cfg(test)]
/// sha3 of the schema: every object's type, name, table and SQL.
pub(crate) fn shape_hash(conn: &Connection) -> Result<String> {
    let mut stmt =
        conn.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name")?;
    let mut rows = stmt.query([])?;
    let mut hash = Sha3_256::new();
    while let Some(row) = rows.next()? {
        for i in 0..4 {
            let field: Option<String> = row.get(i)?;
            hash.update(field.unwrap_or_default().as_bytes());
            hash.update([0x1f]);
        }
        hash.update([0x1e]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
/// `init_db`'s body, from its signature to its closing brace.
fn init_db_body(source: &str) -> String {
    let start = source
        .find("pub fn init_db(")
        .expect("init_db in sqlite.rs");
    let len = source[start..]
        .find("\n}\n")
        .expect("init_db's closing brace");
    source[start..start + len + 2].to_string()
}

#[cfg(test)]
/// The body's hash with every statement that sets a pragma or reads the cache
/// size left out, comments before it included, so tuning a pragma in place
/// never trips the pin.
fn hash_without_pragmas(body: &str) -> String {
    let kept: Vec<&str> = body
        .split_inclusive(";\n")
        .filter(|stmt| !stmt.contains("pragma_update") && !stmt.contains("cache_kib"))
        .collect();
    hex::encode(Sha3_256::digest(kept.concat().as_bytes()))
}

#[cfg(test)]
fn init_db_body_hash() -> String {
    hash_without_pragmas(&init_db_body(include_str!("../sqlite.rs")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::binary_version;
    use crate::db::sqlite;

    const FROZEN: &str = "init_db is migration 1 and frozen: add \
                          `migrations/{sqlite,postgres}/NNNN_*.sql` (docs/database.md)";

    fn versions(conn: &Connection) -> Vec<(i64, String)> {
        conn.prepare("SELECT version, checksum FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn has_table(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [name],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Pin A: the schema `init_db` builds.
    #[test]
    fn pin_a_init_db_builds_the_baseline_shape() {
        let conn = sqlite::init_db(":memory:").unwrap();
        assert_eq!(shape_hash(&conn).unwrap(), BASELINE_SHA3, "{FROZEN}");
    }

    /// Pin B: the rest of `init_db`, such as its legacy DROPs and the counter
    /// seeding, which a fresh database cannot show. Pragmas stay tunable.
    #[test]
    fn pin_b_init_db_body_is_unchanged() {
        assert_eq!(init_db_body_hash(), INIT_DB_BODY_SHA3, "{FROZEN}");
    }

    #[test]
    fn pin_b_lets_pragmas_be_tuned() {
        let body = init_db_body(include_str!("../sqlite.rs"));
        let tuned = body
            .replace("512 * 1024", "256 * 1024")
            .replace("\"NORMAL\"", "\"FULL\"");
        assert_ne!(body, tuned, "the replacements must hit");
        assert_eq!(hash_without_pragmas(&body), hash_without_pragmas(&tuned));
        let edited = body.replace("DROP", "DROP  ");
        assert_ne!(hash_without_pragmas(&body), hash_without_pragmas(&edited));
    }

    #[test]
    fn a_fresh_file_is_stamped_at_this_binarys_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.db");
        let db = sqlite::open(path.to_str().unwrap()).unwrap();
        let rows = versions(&sqlite::lock(&db));
        assert_eq!(rows.first(), Some(&(1, BASELINE_SHA3.to_string())));
        assert_eq!(rows.len() as i64, binary_version());
    }

    /// A file written before versions existed is adopted as version 1, rows
    /// and all, and reopening it changes nothing.
    #[test]
    fn a_legacy_file_is_adopted_with_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        {
            let conn = sqlite::init_db(path.to_str().unwrap()).unwrap();
            conn.execute(
                "INSERT INTO kv (key, value, updated_at) VALUES ('stats', '{}', 1)",
                [],
            )
            .unwrap();
            assert!(!has_table(&conn, "schema_migrations"));
        }
        for _ in 0..2 {
            let db = sqlite::open(path.to_str().unwrap()).unwrap();
            let conn = sqlite::lock(&db);
            assert_eq!(versions(&conn)[0], (1, BASELINE_SHA3.to_string()));
            assert_eq!(versions(&conn).len() as i64, binary_version());
            let kept: String = conn
                .query_row("SELECT value FROM kv WHERE key = 'stats'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(kept, "{}");
        }
    }

    #[test]
    fn a_database_newer_than_the_binary_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("newer.db");
        drop(sqlite::open(path.to_str().unwrap()).unwrap());
        let newer = binary_version() + 1;
        Connection::open(&path)
            .unwrap()
            .execute(
                "INSERT INTO schema_migrations VALUES (?1, 'later', 'x', 0, 'test')",
                [newer],
            )
            .unwrap();

        let err = format!("{:#}", sqlite::open(path.to_str().unwrap()).err().unwrap());
        assert!(err.contains(&format!("version {newer}")), "{err}");
        assert!(err.contains("newer than this binary"), "{err}");
    }

    #[test]
    fn an_edited_migration_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edited.db");
        drop(sqlite::open(path.to_str().unwrap()).unwrap());
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 1",
                [],
            )
            .unwrap();

        let err = format!("{:#}", sqlite::open(path.to_str().unwrap()).err().unwrap());
        assert!(
            err.contains("migration 1 was edited after it was applied"),
            "{err}"
        );
    }

    /// A legacy file that has drifted keeps today's error, and nothing is
    /// stamped: the check runs inside the runner's transaction.
    #[test]
    fn a_drifted_legacy_file_is_refused_and_left_unstamped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drifted.db");
        sqlite::init_db(path.to_str().unwrap())
            .unwrap()
            .execute_batch("CREATE INDEX idx_drift ON blocks (miner)")
            .unwrap();

        let err = format!("{:#}", sqlite::open(path.to_str().unwrap()).err().unwrap());
        assert!(err.contains("idx_drift"), "{err}");
        let conn = Connection::open(&path).unwrap();
        assert!(!has_table(&conn, "schema_migrations"));
    }

    /// The commute rule: `init_db` runs on every open, so after each version a
    /// second `init_db` must change nothing.
    #[test]
    fn every_migration_commutes_with_init_db() {
        for upto in 2..=binary_version() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("commute.db");
            let conn = sqlite::init_db(path.to_str().unwrap()).unwrap();
            for m in &crate::db::migrations::MIGRATIONS[1..upto as usize] {
                conn.execute_batch(m.sqlite.expect("a SQLite twin"))
                    .unwrap();
            }
            let before = shape_hash(&conn).unwrap();
            drop(conn);
            let again = sqlite::init_db(path.to_str().unwrap()).unwrap();
            assert_eq!(
                shape_hash(&again).unwrap(),
                before,
                "migration {upto} does not commute with init_db (docs/database.md)"
            );
        }
    }
}
