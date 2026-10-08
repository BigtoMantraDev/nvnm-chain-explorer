//! The Postgres runner, its lock-free preflight, and the web role's grants.

use anyhow::{anyhow, Result};
use sqlx::{AssertSqlSafe, Connection, PgConnection, Row};

use super::q;
use super::DbError;
use crate::db::config::is_identifier;
use crate::db::migrations::{
    binary_version, checksum, concurrent_indexes, first_gap, headers, postgres_checksums,
    split_at_line_end, Migration, MIGRATIONS,
};
use crate::db::now_ts;

const APPLIED_BY: &str = concat!("nvnmchain-explorer ", env!("CARGO_PKG_VERSION"));

pub(crate) enum PreflightError {
    /// The database is not one this binary may write: the process exits.
    Refused(anyhow::Error),
    /// It could not be asked; retried with backoff.
    Unavailable(anyhow::Error),
}

impl From<sqlx::Error> for PreflightError {
    fn from(e: sqlx::Error) -> Self {
        PreflightError::Unavailable(e.into())
    }
}

/// What the indexer refuses before it takes the lock, so a broken image turns
/// unready, or exits, and never replaces a working leader:
///
/// - a schema with tables but no `schema_migrations`, i.e. applied by hand;
/// - a version missing below the highest, i.e. rows edited by hand;
/// - a migration whose file changed after it was applied;
/// - a database newer than this binary (D > B);
/// - an INVALID index, unless `REINDEX CONCURRENTLY` is building it or a
///   pending no-transaction file will drop and rebuild it.
pub(crate) async fn preflight(conn: &mut PgConnection) -> Result<(), PreflightError> {
    let row = sqlx::query(
        "SELECT to_regclass('schema_migrations') IS NOT NULL, \
         (SELECT COUNT(*) FROM pg_tables WHERE schemaname = current_schema())",
    )
    .fetch_one(&mut *conn)
    .await?;
    let (versioned, tables): (bool, i64) = (row.try_get(0)?, row.try_get(1)?);
    if !versioned {
        if tables > 0 {
            return Err(PreflightError::Refused(anyhow!(
                "the schema has {tables} table(s) but no schema_migrations: it was applied by \
                 hand. Drop the schema and re-index from the chain"
            )));
        }
        return Ok(());
    }
    let applied: Vec<(i64, String)> =
        sqlx::query("SELECT version, checksum FROM schema_migrations ORDER BY version")
            .fetch_all(&mut *conn)
            .await?
            .iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?)))
            .collect::<Result<_, sqlx::Error>>()?;
    if let Some((missing, found)) = first_gap(applied.iter().map(|(v, _)| *v)) {
        return Err(PreflightError::Refused(anyhow!(
            "schema_migrations has version {found} but not {missing}: its rows were edited by \
             hand, so which files ran is unknown. Drop the schema and re-index from the chain"
        )));
    }
    let binary = binary_version();
    let db = applied.last().map_or(0, |(v, _)| *v);
    if db > binary {
        return Err(PreflightError::Refused(anyhow!(
            "the database is at schema version {db}, newer than this binary's {binary}; \
             deploy a release at version {db} or later (the only way back is forward)"
        )));
    }
    let sums = postgres_checksums();
    for (version, stored) in &applied {
        let expected = usize::try_from(*version - 1).ok().and_then(|i| sums.get(i));
        if expected != Some(stored) {
            return Err(PreflightError::Refused(anyhow!(
                "migration {version} was edited after it was applied"
            )));
        }
    }
    let rebuilding: Vec<String> = MIGRATIONS[db as usize..]
        .iter()
        .filter(|m| headers(m.postgres).0.no_transaction)
        .flat_map(|m| concurrent_indexes(m.postgres))
        .collect();
    let invalid: Vec<String> = sqlx::query(
        "SELECT c.relname FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = current_schema() AND NOT i.indisvalid",
    )
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .map(|r| r.try_get::<String, _>(0))
    .collect::<Result<_, _>>()?;
    let blocking: Vec<&String> = invalid
        .iter()
        .filter(|name| !name.contains("_ccnew") && !name.contains("_ccold"))
        .filter(|name| !rebuilding.contains(name))
        .collect();
    if !blocking.is_empty() {
        return Err(PreflightError::Refused(anyhow!(
            "INVALID index(es) {blocking:?}: drop and rebuild them (DROP INDEX CONCURRENTLY, \
             then the CREATE from their migration) before starting"
        )));
    }
    Ok(())
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> DbError {
    DbError::Unavailable(anyhow!("{what}: {e}"))
}

/// Apply the pending versions on the writer session, which holds the lock,
/// then re-apply the web role's grants. Any failure is `Unavailable`: the
/// session is dropped and the whole run retried, never the writes after it.
pub(crate) async fn run(conn: &mut PgConnection, web_role: &str) -> Result<(), DbError> {
    q::raw(
        conn,
        "schema_migrations",
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
            applied_at BIGINT NOT NULL, applied_by TEXT NOT NULL)",
    )
    .await
    .map_err(|e| unavailable("create schema_migrations", e))?;
    let db: i64 = q::fetch_one(
        conn,
        "schema version",
        sqlx::query("SELECT COALESCE(MAX(version), 0) FROM schema_migrations"),
    )
    .await
    .map_err(|e| unavailable("read the schema version", e))?
    .try_get(0)
    .map_err(|e| unavailable("read the schema version", e))?;
    // The writer re-runs the preflight under the lock, so D <= B here.
    let pending = usize::try_from(db)
        .ok()
        .and_then(|d| MIGRATIONS.get(d..))
        .ok_or_else(|| {
            unavailable(
                "schema version",
                format!("{db} is newer than this binary's {}", binary_version()),
            )
        })?;
    for m in pending {
        apply(conn, m).await?;
        tracing::info!("applied migration {:04}_{}", m.version, m.name);
    }
    grant(conn, web_role)
        .await
        .map_err(|e| unavailable(&format!("grant the web role {web_role:?}"), e))
}

async fn apply(conn: &mut PgConnection, m: &'static Migration) -> Result<(), DbError> {
    let no_transaction = headers(m.postgres).0.no_transaction;
    let mut tries = 0;
    loop {
        tries += 1;
        let r = if no_transaction {
            apply_concurrently(conn, m).await
        } else {
            apply_in_transaction(conn, m).await
        };
        match r {
            Ok(()) => return Ok(()),
            // A lock timeout, or a cancelled concurrent build: the file is
            // safe to re-run from the top.
            Err(e)
                if tries < 5
                    && (e.contains("55P03") || (no_transaction && e.contains("57014"))) =>
            {
                tracing::warn!("migration {:04}: {e}; retrying", m.version);
            }
            Err(e) => {
                return Err(unavailable(
                    &format!("migration {:04}_{}", m.version, m.name),
                    e,
                ))
            }
        }
    }
}

fn describe(e: &sqlx::Error) -> String {
    format!("{e} {}", super::error::sqlstate(e).unwrap_or_default())
}

async fn record(conn: &mut PgConnection, m: &Migration) -> Result<(), sqlx::Error> {
    q::count_statement();
    sqlx::query(
        "INSERT INTO schema_migrations (version, name, checksum, applied_at, applied_by) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(m.version)
    .bind(m.name)
    .bind(checksum(m.postgres))
    .bind(now_ts())
    .bind(APPLIED_BY)
    .execute(conn)
    .await
    .map(|_| ())
}

async fn apply_in_transaction(
    conn: &mut PgConnection,
    m: &'static Migration,
) -> Result<(), String> {
    let mut tx = conn.begin().await.map_err(|e| describe(&e))?;
    sqlx::raw_sql("SET LOCAL statement_timeout = 0; SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await
        .map_err(|e| describe(&e))?;
    q::count_statement();
    sqlx::raw_sql(m.postgres)
        .execute(&mut *tx)
        .await
        .map_err(|e| describe(&e))?;
    record(&mut tx, m).await.map_err(|e| describe(&e))?;
    tx.commit().await.map_err(|e| describe(&e))
}

/// Each statement on its own, autocommitted: one multi-statement query is an
/// implicit transaction, which `CONCURRENTLY` refuses. The version row goes in
/// only after the last statement; a re-run starts again from the file's DROP.
async fn apply_concurrently(conn: &mut PgConnection, m: &'static Migration) -> Result<(), String> {
    sqlx::raw_sql("SET statement_timeout = 0; SET lock_timeout = '5s'")
        .execute(&mut *conn)
        .await
        .map_err(|e| describe(&e))?;
    let r = async {
        for stmt in split_at_line_end(m.postgres) {
            q::count_statement();
            sqlx::raw_sql(AssertSqlSafe(stmt))
                .execute(&mut *conn)
                .await
                .map_err(|e| describe(&e))?;
        }
        record(conn, m).await.map_err(|e| describe(&e))
    }
    .await;
    sqlx::raw_sql("RESET statement_timeout; RESET lock_timeout")
        .execute(&mut *conn)
        .await
        .map_err(|e| describe(&e))?;
    r
}

/// What web replicas may do: read everything, and write the two caches. Run
/// after every start, since a migration that recreates a table drops its
/// grants. A missing role is a warning, not an error: docker-compose and CI
/// create only `explorer`. Any other failure is returned, so the writer never
/// leads without its web role's grants.
async fn grant(conn: &mut PgConnection, web_role: &str) -> Result<(), String> {
    let row =
        sqlx::query("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1), current_schema()")
            .bind(web_role)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| describe(&e))?;
    let exists: bool = row.try_get(0).map_err(|e| describe(&e))?;
    let schema: String = row.try_get(1).map_err(|e| describe(&e))?;
    if !exists {
        tracing::warn!("web role {web_role:?} does not exist; skipping its grants");
        return Ok(());
    }
    if !is_identifier(web_role) || !is_identifier(&schema) {
        return Err(format!(
            "not plain identifiers: role {web_role:?}, schema {schema:?}"
        ));
    }
    let sql = format!(
            "GRANT USAGE ON SCHEMA \"{schema}\" TO \"{web_role}\";
             GRANT SELECT ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{web_role}\";
             GRANT INSERT, UPDATE ON selector_names TO \"{web_role}\";
             GRANT UPDATE (trace_data) ON transactions TO \"{web_role}\";
             ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT SELECT ON TABLES TO \"{web_role}\""
    );
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .map_err(|e| describe(&e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-transaction file builds its index concurrently and records its
    /// version, and a re-run after an interrupted build (an INVALID index left
    /// behind) drops and rebuilds it. Needs `PG_TEST_URL`.
    #[tokio::test]
    #[ignore = "needs PG_TEST_URL; see AGENTS.md"]
    async fn a_no_transaction_file_rebuilds_an_interrupted_index() {
        let url = std::env::var("PG_TEST_URL").expect("PG_TEST_URL");
        // The pattern the test helpers' sweep removes once this process is gone.
        let schema = format!("t_{}_900000", std::process::id());
        let mut admin = PgConnection::connect(&url).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        // The writer's own statement_timeout, which the runner must restore.
        let mut conn = PgConnection::connect(&format!(
            "{url}?options[search_path]={schema}&options[statement_timeout]=60s"
        ))
        .await
        .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL,
                 checksum TEXT NOT NULL, applied_at BIGINT NOT NULL, applied_by TEXT NOT NULL);
             CREATE TABLE blocks (number BIGINT PRIMARY KEY, miner BYTEA);
             INSERT INTO blocks VALUES (1, '\\x01'), (2, '\\x01');
             CREATE INDEX idx_miner ON blocks (miner);
             UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'idx_miner'::regclass;",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        let file: &'static Migration = Box::leak(Box::new(Migration {
            version: 2,
            name: "miner_index",
            sqlite: Some("-- kind: expand\n"),
            postgres: "-- kind: expand\n-- no-transaction\n\
                       DROP INDEX CONCURRENTLY IF EXISTS idx_miner;\n\
                       CREATE INDEX CONCURRENTLY idx_miner\n    ON blocks (miner);\n",
        }));
        apply(&mut conn, file).await.unwrap();
        let (valid, versions): (bool, i64) = sqlx::query_as(
            "SELECT (SELECT indisvalid FROM pg_index WHERE indexrelid = 'idx_miner'::regclass),
                    (SELECT COUNT(*) FROM schema_migrations WHERE version = 2)",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(valid, "rebuilt valid");
        assert_eq!(versions, 1);
        let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(timeout, "1min", "the session's own settings are restored");
        drop(conn);
        sqlx::raw_sql(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&mut admin)
            .await
            .unwrap();
    }

    /// A grant that fails for any reason but a missing role fails the run, so
    /// the writer never leads without its web role's grants, and the next run
    /// grants again. Needs `PG_TEST_URL`.
    #[tokio::test]
    #[ignore = "needs PG_TEST_URL; see AGENTS.md"]
    async fn a_failed_grant_fails_the_run() {
        let url = std::env::var("PG_TEST_URL").expect("PG_TEST_URL");
        let schema = format!("t_{}_900001", std::process::id());
        let role = format!("t_grant_{}", std::process::id());
        let mut admin = PgConnection::connect(&url).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};
             DROP ROLE IF EXISTS {role}; CREATE ROLE {role}"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
        let mut conn = PgConnection::connect(&format!("{url}?options[search_path]={schema}"))
            .await
            .unwrap();
        run(&mut conn, &role).await.unwrap();

        // On a healthy session, a grant that cannot apply: a column it names
        // is gone.
        sqlx::raw_sql("ALTER TABLE transactions RENAME COLUMN trace_data TO trace_data_gone")
            .execute(&mut conn)
            .await
            .unwrap();
        let err = run(&mut conn, &role)
            .await
            .expect_err("a failed grant fails the run");
        assert!(matches!(err, DbError::Unavailable(_)), "{err}");
        assert!(err.to_string().contains("42703"), "{err}");

        sqlx::raw_sql(AssertSqlSafe(format!(
            "ALTER TABLE transactions RENAME COLUMN trace_data_gone TO trace_data;
             REVOKE ALL ON transactions FROM {role}"
        )))
        .execute(&mut conn)
        .await
        .unwrap();
        run(&mut conn, &role).await.unwrap();
        let granted: bool = sqlx::query_scalar(
            "SELECT has_column_privilege($1, 'transactions', 'trace_data', 'UPDATE')",
        )
        .bind(&role)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(granted, "the next run grants again");
        drop(conn);
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA {schema} CASCADE; DROP OWNED BY {role}; DROP ROLE {role}"
        )))
        .execute(&mut admin)
        .await
        .unwrap();
    }
}
