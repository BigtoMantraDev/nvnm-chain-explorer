//! SQLite queries added since the move to `db/sqlite.rs`. New queries go here
//! rather than into `sqlite.rs`, so the bodies teammates edit stay as they are.

use anyhow::{Context, Result};

use super::{lock, row_to_token, Db, TOKEN_COLS};
use crate::models::TokenMetadata;

/// Every token-metadata row, or why they could not be read. Unlike
/// `get_all_token_metas`, a failed read is an error, never an empty table.
/// Undecodable rows are still dropped and logged, as there.
pub fn try_all_token_metas(db: &Db) -> Result<Vec<TokenMetadata>> {
    let conn = lock(db);
    let mut stmt = conn
        .prepare(&format!("SELECT {TOKEN_COLS} FROM token_metadata"))
        .context("try_all_token_metas")?;
    let rows = stmt
        .query_map([], row_to_token)
        .context("try_all_token_metas")?;
    let mut out = Vec::new();
    let (mut dropped, mut first) = (0usize, None);
    for row in rows {
        match row {
            Ok(meta) => out.push(meta),
            Err(e) => {
                dropped += 1;
                first.get_or_insert_with(|| e.to_string());
            }
        }
    }
    if let Some(e) = first {
        tracing::warn!("try_all_token_metas: dropped {dropped} undecodable row(s); first: {e}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sqlite;

    fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = sqlite::open(dir.path().join("extra.db").to_str().unwrap()).unwrap();
        (dir, db)
    }

    /// The label cache seeds from this; an empty list would erase every label
    /// where a failed read must leave them be.
    #[test]
    fn a_failed_token_read_is_an_error_not_an_empty_list() {
        let (_dir, db) = temp_db();
        assert!(try_all_token_metas(&db).unwrap().is_empty());

        sqlite::lock(&db)
            .execute_batch("DROP TABLE token_metadata")
            .unwrap();
        assert!(try_all_token_metas(&db).is_err());
    }
}
