//! Versioned migrations, end to end.
//!
//! The SQLite tests need nothing but the fixtures.

use std::collections::BTreeMap;
use std::path::Path;

use nvnmchain_explorer::db;
use rusqlite::{Connection, OpenFlags};

#[allow(dead_code)]
mod common;
use common::baseline::{columns, rows, tables, SPECS};

/// Every table's rows over `cols`, the columns the legacy file had, in a
/// comparable form.
fn contents(conn: &Connection, cols: &BTreeMap<&str, Vec<String>>) -> Vec<(String, usize, String)> {
    let mut out = Vec::new();
    for spec in SPECS {
        let rows = rows(conn, spec, &cols[spec.table]);
        out.push((
            spec.table.to_string(),
            rows.len(),
            format!("{:?}", rows.iter().take(50).collect::<Vec<_>>()),
        ));
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
        let cols: BTreeMap<&str, Vec<String>> = SPECS
            .iter()
            .map(|s| (s.table, columns(&original, s.table)))
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
        assert_eq!(contents(&adopted, &cols), before, "{name}: rows changed");
    }
}
