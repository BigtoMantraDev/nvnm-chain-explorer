//! Every Postgres statement goes through here.
//!
//! Reads keep SQLite's rule: a failed query is logged and becomes "no data",
//! so a page degrades rather than erroring. Here a failure also sets
//! `DB_FAILED`, which the 503 middleware turns into a `503` rather than a false
//! empty page or 404. Each read runs on an explicitly acquired connection
//! under a client deadline, and a connection that times out is closed, never
//! returned to the pool. `try_*` variants return the error instead, for the
//! callers that must not mistake a failure for an empty table.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{PgPool, Postgres};

use super::flag_failure;

pub(crate) type PgQuery = sqlx::query::Query<'static, Postgres, PgArguments>;

/// A read's whole round trip: the server's `statement_timeout` (5 s) plus 2 s,
/// so the server cancels first and the error is clean.
pub(crate) const READ_DEADLINE: Duration = Duration::from_secs(7);
/// A cache write's round trip: `statement_timeout` (2 s) plus 2 s.
pub(crate) const CACHE_DEADLINE: Duration = Duration::from_secs(4);

static STATEMENTS: AtomicU64 = AtomicU64::new(0);

/// Statements sent so far by this process, for the round-trip budget tests.
pub fn statements() -> u64 {
    STATEMENTS.load(Relaxed)
}

pub(crate) fn count_statement() {
    STATEMENTS.fetch_add(1, Relaxed);
}

/// Why a read failed: the database's error, or the deadline.
#[derive(Debug)]
pub(crate) enum ReadError {
    Sql(sqlx::Error),
    Deadline,
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Sql(e) => write!(f, "{e}"),
            ReadError::Deadline => f.write_str("no answer within the client deadline"),
        }
    }
}

impl std::error::Error for ReadError {}

/// Run `$body` on a connection from `$pool` under `$deadline`, with the
/// connection bound to `$c`. A connection that misses the deadline may be
/// half-open, so it is closed rather than returned to the pool.
macro_rules! on_pool {
    ($pool:expr, $deadline:expr, |$c:ident| $body:expr) => {{
        let mut conn = $pool.acquire().await.map_err(ReadError::Sql)?;
        count_statement();
        let $c = &mut *conn;
        match tokio::time::timeout($deadline, $body).await {
            Ok(r) => r.map_err(ReadError::Sql),
            Err(_) => {
                conn.close_on_drop();
                Err(ReadError::Deadline)
            }
        }
    }};
}

fn degrade(what: &str, e: &ReadError) {
    tracing::warn!("{what}: {e}");
    flag_failure();
}

/// Whether a read in this request already failed. The response will be a 503
/// whatever the rest find, so they degrade at once rather than each waiting
/// out the database.
fn already_failed() -> bool {
    super::DB_FAILED.try_with(|f| f.get()).unwrap_or(false)
}

/// Map every row, dropping (and logging) the ones that do not decode, as
/// `sqlite.rs`'s `query_rows` does.
fn map_rows<T>(
    what: &str,
    rows: Vec<PgRow>,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Vec<T> {
    let mut out = Vec::with_capacity(rows.len());
    let (mut dropped, mut first) = (0usize, None);
    for row in &rows {
        match map(row) {
            Ok(v) => out.push(v),
            Err(e) => {
                dropped += 1;
                first.get_or_insert_with(|| e.to_string());
            }
        }
    }
    if let Some(e) = first {
        tracing::warn!("{what}: dropped {dropped} undecodable row(s); first: {e}");
    }
    out
}

pub(crate) async fn try_query_rows<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Result<Vec<T>, ReadError> {
    let q = bind(sqlx::query(sql));
    let rows = on_pool!(pool, READ_DEADLINE, |c| q.fetch_all(c))?;
    Ok(map_rows(what, rows, map))
}

/// Every row, or none (logged, `DB_FAILED` set) when the query fails.
pub(crate) async fn query_rows<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Vec<T> {
    if already_failed() {
        return Vec::new();
    }
    match try_query_rows(pool, what, sql, bind, map).await {
        Ok(rows) => rows,
        Err(e) => {
            degrade(what, &e);
            Vec::new()
        }
    }
}

pub(crate) async fn try_query_opt<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Result<Option<T>, ReadError> {
    let q = bind(sqlx::query(sql));
    let row = on_pool!(pool, READ_DEADLINE, |c| q.fetch_optional(c))?;
    Ok(match row {
        None => None,
        Some(row) => match map(&row) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!("{what}: dropped 1 undecodable row(s); first: {e}");
                None
            }
        },
    })
}

/// The first row, or `None` when there is none or the query fails.
pub(crate) async fn query_opt<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Option<T> {
    if already_failed() {
        return None;
    }
    match try_query_opt(pool, what, sql, bind, map).await {
        Ok(v) => v,
        Err(e) => {
            degrade(what, &e);
            None
        }
    }
}

/// A single `int8`, or 0 when the query fails.
pub(crate) async fn query_count(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
) -> i64 {
    query_opt(pool, what, sql, bind, |r| {
        sqlx::Row::try_get::<i64, _>(r, 0)
    })
    .await
    .unwrap_or(0)
}

/// A write to a true cache (selector names, traces), on the cache pool. A
/// failure is logged and returned, and never sets `DB_FAILED`: the page that
/// asked has its answer either way.
pub(crate) async fn exec_best_effort(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
) -> anyhow::Result<()> {
    let q = bind(sqlx::query(sql));
    let run = async { on_pool!(pool, CACHE_DEADLINE, |c| q.execute(c)) };
    match run.await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!("{what}: {e}");
            Err(anyhow::anyhow!("{what}: {e}"))
        }
    }
}
