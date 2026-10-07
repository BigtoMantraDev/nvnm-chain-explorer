//! Versioned migrations, end to end.
//!
//! The SQLite tests need nothing but the fixtures.

use std::collections::BTreeMap;
use std::path::Path;

use nvnmchain_explorer::db;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags};

#[allow(dead_code)]
mod common;
use common::baseline::{columns, tables};

/// A value as stored, its storage class included: a rewrite to equal-looking
/// text or JSON still shows.
fn exact(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{f:?}"),
        Value::Text(s) => format!("{s:?}"),
        Value::Blob(b) => format!("x'{}'", hex::encode(b)),
    }
}

/// Every row of every table in `cols`, over its columns there (the ones the
/// legacy file had), sorted so that only the rows themselves are compared.
fn contents(
    conn: &Connection,
    cols: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for (table, cols) in cols {
        let list = cols
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut stmt = conn
            .prepare(&format!("SELECT {list} FROM \"{table}\""))
            .unwrap();
        let mut rows: Vec<String> = stmt
            .query_map([], |r| {
                (0..cols.len())
                    .map(|i| Ok(exact(r.get(i)?)))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .map(|row| format!("({})", row.unwrap().join(", ")))
            .collect();
        rows.sort();
        out.insert(table.clone(), rows);
    }
    out
}

/// What changed in each table, with a few rows to show it.
fn changes(
    before: &BTreeMap<String, Vec<String>>,
    after: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (table, was) in before {
        let now = &after[table];
        if now != was {
            let only = |a: &[String], b: &[String]| -> Vec<String> {
                a.iter()
                    .filter(|r| b.binary_search(r).is_err())
                    .take(3)
                    .cloned()
                    .collect()
            };
            out.push(format!(
                "{table}: {} rows before, {} after; gone {:?}; new {:?}",
                was.len(),
                now.len(),
                only(was, now),
                only(now, was)
            ));
        }
    }
    out
}

/// A deployed file written before versions existed is adopted as version 1
/// when the new binary opens it, and every later version applies over it with
/// its rows intact (the per-version data upgrade): the canaries stay legacy on
/// disk, so this runs on every build.
#[tokio::test]
async fn both_canaries_are_adopted_with_their_rows_intact() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/baseline");
    for name in ["canary-blocks.db", "canary-rich.db"] {
        let original = Connection::open_with_flags(
            root.join(name),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        assert!(
            !tables(&original).contains(&"schema_migrations".to_string()),
            "{name} must stay a legacy file"
        );
        let cols: BTreeMap<String, Vec<String>> = tables(&original)
            .into_iter()
            .filter(|t| !t.starts_with("sqlite_"))
            .map(|t| {
                let cols = columns(&original, &t);
                (t, cols)
            })
            .collect();
        let before = contents(&original, &cols);

        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join(name);
        original
            .execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
            .unwrap();
        drop(original);
        let opened = db::open(copy.to_str().unwrap()).await.unwrap();
        drop(opened);

        let adopted = Connection::open(&copy).unwrap();
        let stamped: Vec<(i64, String)> = adopted
            .prepare("SELECT version, name FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(stamped[0], (1, "baseline".to_string()), "{name}");
        assert_eq!(
            stamped.len() as i64,
            db::migrations::binary_version(),
            "{name}"
        );
        let changed = changes(&before, &contents(&adopted, &cols));
        assert!(
            changed.is_empty(),
            "{name}: rows changed:\n{}",
            changed.join("\n")
        );
    }
}
