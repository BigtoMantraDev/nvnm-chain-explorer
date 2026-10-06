//! What a Postgres failure means to the writer.
//!
//! `Unavailable` is the default: anything not known to depend on a write's
//! content is retried until it succeeds, because a dropped block would be a
//! permanent hole (backfill only walks below `MIN(number)`). Only `Data`
//! reaches the caller, where the indexer's per-bundle fallback isolates the
//! bundle that caused it.

use std::fmt;

/// How the writer treats a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// Depends on the write's content: returned to the caller.
    Data,
    /// A serialization failure or deadlock: retried at once.
    Retry,
    /// The database, not the write: the session is dropped and the same write
    /// retried with backoff, never returned.
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Database,
    Encode,
    Other,
}

pub(crate) fn class_of(code: Option<&str>, kind: Kind) -> Class {
    if kind == Kind::Encode {
        return Class::Data;
    }
    match code {
        Some(c) if c.starts_with("22") || c.starts_with("23") || c == "21000" => Class::Data,
        Some("40001" | "40P01") => Class::Retry,
        _ => Class::Unavailable,
    }
}

pub(crate) fn sqlstate(e: &sqlx::Error) -> Option<String> {
    e.as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.into_owned())
}

pub(crate) fn classify(e: &sqlx::Error) -> Class {
    let kind = match e {
        sqlx::Error::Database(_) => Kind::Database,
        sqlx::Error::Encode(_) => Kind::Encode,
        _ => Kind::Other,
    };
    class_of(sqlstate(e).as_deref(), kind)
}

/// A Postgres write's failure, as the writer reports it.
#[derive(Debug)]
pub enum DbError {
    /// The write's content was refused; the same write will fail again.
    Data(anyhow::Error),
    /// The database is unreachable or refused for a reason of its own. The
    /// writer retries these itself and never returns one from `write`.
    Unavailable(anyhow::Error),
    /// A serialization failure or deadlock, retried at once.
    Retry(anyhow::Error),
    /// A chain-derived write in a process with no writer (`ROLE=web`).
    NotWriter,
    /// The writer must stop: the process exits with this code.
    Fatal(i32),
}

impl DbError {
    pub(crate) fn from_sqlx(what: &str, e: sqlx::Error) -> Self {
        let code = sqlstate(&e).unwrap_or_default();
        let err = anyhow::anyhow!("{what}: {e} {code}");
        match classify(&e) {
            Class::Data => DbError::Data(err),
            Class::Retry => DbError::Retry(err),
            Class::Unavailable => DbError::Unavailable(err),
        }
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Data(e) => write!(f, "{e:#}"),
            DbError::Unavailable(e) => write!(f, "database unavailable: {e:#}"),
            DbError::Retry(e) => write!(f, "{e:#}"),
            DbError::NotWriter => {
                f.write_str("this process has no writer (ROLE=web); the indexer writes chain data")
            }
            DbError::Fatal(code) => write!(f, "the writer stopped the process (exit {code})"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<tokio::task::JoinError> for DbError {
    fn from(e: tokio::task::JoinError) -> Self {
        match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => DbError::Unavailable(anyhow::anyhow!("writer task: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_errors_are_data() {
        for code in ["22003", "22P02", "23505", "23503", "21000"] {
            assert_eq!(class_of(Some(code), Kind::Database), Class::Data, "{code}");
        }
        assert_eq!(class_of(None, Kind::Encode), Class::Data);
    }

    #[test]
    fn conflicts_are_retried_at_once() {
        for code in ["40001", "40P01"] {
            assert_eq!(class_of(Some(code), Kind::Database), Class::Retry, "{code}");
        }
    }

    /// Privileges, missing tables, a full disk, a read-only server, internal
    /// errors and cancellations are about the database, not the block.
    #[test]
    fn everything_else_is_unavailable() {
        for code in [
            "42501", "42P01", "53100", "25006", "XX000", "57014", "57P01", "08006", "55P03",
        ] {
            assert_eq!(
                class_of(Some(code), Kind::Database),
                Class::Unavailable,
                "{code}"
            );
        }
        assert_eq!(class_of(None, Kind::Other), Class::Unavailable);
        assert_eq!(
            classify(&sqlx::Error::Protocol("unexpected message".into())),
            Class::Unavailable
        );
        assert_eq!(classify(&sqlx::Error::PoolTimedOut), Class::Unavailable);
    }

    #[test]
    fn an_encode_error_is_data() {
        let e = sqlx::Error::Encode("value too large".into());
        assert_eq!(classify(&e), Class::Data);
    }
}
