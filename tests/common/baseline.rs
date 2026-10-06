//! How a baseline's rows are compared, shared by the re-index check
//! (`tests/baseline.rs`), the Postgres round-trip (`tests/postgres.rs`), and
//! the other suites that compare tables, on one backend or across both.

use std::collections::BTreeMap;

use rusqlite::types::Value as Sql;
use rusqlite::Connection;
use serde_json::{json, Value};
use sqlx::postgres::PgConnection;
use sqlx::{AssertSqlSafe, Column as _, Row as _, TypeInfo as _};

/// How each table is compared: the natural key rows are matched on, and the
/// columns that legitimately differ between two runs over the same blocks.
pub struct Spec {
    pub table: &'static str,
    pub key: &'static [&'static str],
    pub skip: &'static [&'static str],
}

pub const SPECS: &[Spec] = &[
    // `created_at` is the wall clock at write time.
    Spec {
        table: "blocks",
        key: &["number"],
        skip: &["created_at"],
    },
    // `trace_data` is only written when a page asks for it.
    Spec {
        table: "transactions",
        key: &["hash"],
        skip: &["created_at", "trace_data"],
    },
    // `id` follows insert order, which batching and concurrency choose.
    Spec {
        table: "transfer_events",
        key: &["block_number", "log_index"],
        skip: &["id", "created_at"],
    },
    Spec {
        table: "anchoring_events",
        key: &["block_number", "log_index"],
        skip: &[],
    },
    // `total_supply` is read at the chain's latest state, not at the block.
    Spec {
        table: "token_metadata",
        key: &["address"],
        skip: &["total_supply", "created_at", "updated_at"],
    },
    Spec {
        table: "token_balances",
        key: &["token_addr", "holder_addr"],
        skip: &["updated_at"],
    },
    Spec {
        table: "genesis_balances",
        key: &["token_addr", "holder_addr"],
        skip: &[],
    },
    Spec {
        table: "counters",
        key: &["name"],
        skip: &[],
    },
];

pub fn columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .unwrap();
    let names = stmt.query_map([], |r| r.get(0)).unwrap();
    names.map(Result::unwrap).collect()
}

pub fn tables(conn: &Connection) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    let names = stmt.query_map([], |r| r.get(0)).unwrap();
    names.map(Result::unwrap).collect()
}

pub fn to_json(column: &str, value: Sql) -> Value {
    match value {
        Sql::Null => Value::Null,
        Sql::Integer(i) => json!(i),
        Sql::Real(f) => json!(f),
        // Compared as data rather than text, so key order cannot differ.
        Sql::Text(s) if column == "receipt_data" => serde_json::from_str(&s).unwrap_or(json!(s)),
        Sql::Text(s) => json!(s),
        Sql::Blob(b) => json!(format!("0x{}", hex::encode(b))),
    }
}

/// A table's rows by key, each row its compared columns.
pub type Rows = BTreeMap<String, BTreeMap<String, Value>>;

/// The key a row is matched on: its `spec.key` values, joined.
pub fn row_key(spec: &Spec, fields: &BTreeMap<String, Value>) -> String {
    spec.key
        .iter()
        .map(|k| fields[*k].to_string())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn rows(conn: &Connection, spec: &Spec, cols: &[String]) -> Rows {
    let list = cols
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .prepare(&format!("SELECT {list} FROM {}", spec.table))
        .unwrap();
    let mut out = Rows::new();
    let mut found = stmt.query([]).unwrap();
    while let Some(row) = found.next().unwrap() {
        let mut fields = BTreeMap::new();
        for (i, col) in cols.iter().enumerate() {
            fields.insert(col.clone(), to_json(col, row.get(i).unwrap()));
        }
        out.insert(row_key(spec, &fields), fields);
    }
    out
}

/// Every row-level difference between `base` and `new`, labelled by side.
pub fn diff_rows(table: &str, base: &Rows, new: &Rows, sides: (&str, &str)) -> Vec<String> {
    let mut diffs = Vec::new();
    for (key, row) in base {
        match new.get(key) {
            None => diffs.push(format!("{table} {key}: only in the {}", sides.0)),
            Some(other) => {
                for (col, value) in row {
                    if other[col] != *value {
                        diffs.push(format!(
                            "{table} {key}: {col} was {value}, now {}",
                            other[col]
                        ));
                    }
                }
            }
        }
    }
    for key in new.keys().filter(|k| !base.contains_key(*k)) {
        diffs.push(format!("{table} {key}: only in the {}", sides.1));
    }
    diffs
}

fn quoted(cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A table's rows read back, converted with the same rules as the fixture's.
pub async fn pg_rows(conn: &mut PgConnection, spec: &Spec, cols: &[String]) -> Rows {
    let found = sqlx::query(AssertSqlSafe(format!(
        "SELECT {} FROM {}",
        quoted(cols),
        spec.table
    )))
    .fetch_all(conn)
    .await
    .unwrap_or_else(|e| panic!("{}: read back: {e}", spec.table));
    let mut out = Rows::new();
    for row in &found {
        let mut fields = BTreeMap::new();
        for (i, col) in cols.iter().enumerate() {
            let value = match row.columns()[i].type_info().name() {
                "INT8" => row.get::<Option<i64>, _>(i).map_or(Sql::Null, Sql::Integer),
                "TEXT" => row.get::<Option<String>, _>(i).map_or(Sql::Null, Sql::Text),
                "BYTEA" => row
                    .get::<Option<Vec<u8>>, _>(i)
                    .map_or(Sql::Null, Sql::Blob),
                ty => panic!("{}.{col}: unexpected type {ty}", spec.table),
            };
            fields.insert(col.clone(), to_json(col, value));
        }
        out.insert(row_key(spec, &fields), fields);
    }
    assert_eq!(out.len(), found.len(), "{}: duplicate row keys", spec.table);
    out
}
