//! Which database a process opens, and in which role, from its environment.

use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use serde::Serialize;

/// What this process does. `All` is today's single process; production runs
/// `Indexer` (one writer) and `Web` (N replicas) against Postgres.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    All,
    Web,
    Indexer,
}

impl FromStr for Role {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Role::All),
            "web" => Ok(Role::Web),
            "indexer" => Ok(Role::Indexer),
            other => bail!("ROLE={other:?}: expected all, web or indexer"),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Role::All => "all",
            Role::Web => "web",
            Role::Indexer => "indexer",
        })
    }
}

/// The database to open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbTarget {
    /// A SQLite file path, from `DB_PATH`.
    Sqlite(String),
}

/// Everything `db::open_with` needs from the environment.
#[derive(Clone, Debug)]
pub struct DbConfig {
    pub role: Role,
    pub target: DbTarget,
}

impl DbConfig {
    /// Read the configuration through `lookup`, which is `std::env::var` in
    /// production and a fake environment in tests.
    pub fn from_env(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let set = |key: &str| lookup(key).filter(|v| !v.trim().is_empty());
        let role = match set("ROLE") {
            Some(raw) => raw.parse().context("ROLE")?,
            None => Role::All,
        };
        let target = DbTarget::Sqlite(set("DB_PATH").unwrap_or_else(|| "explorer.db".into()));
        if role != Role::All {
            bail!(
                "ROLE={role} needs Postgres. A SQLite file has one process, which runs as ROLE=all"
            );
        }
        Ok(DbConfig { role, target })
    }

    /// A SQLite configuration, as `db::open(path)` uses.
    pub fn sqlite(path: &str) -> Self {
        DbConfig {
            role: Role::All,
            target: DbTarget::Sqlite(path.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn the_default_is_one_process_on_a_sqlite_file() {
        let cfg = DbConfig::from_env(env(&[])).unwrap();
        assert_eq!(cfg.role, Role::All);
        assert_eq!(cfg.target, DbTarget::Sqlite("explorer.db".into()));
    }

    /// Fly, Render and the systemd unit set only `DB_PATH`.
    #[test]
    fn db_path_alone_still_opens_the_file() {
        let cfg = DbConfig::from_env(env(&[("DB_PATH", "/data/explorer.db")])).unwrap();
        assert_eq!(cfg.target, DbTarget::Sqlite("/data/explorer.db".into()));
    }

    #[test]
    fn a_role_is_read_from_role() {
        for (raw, role) in [("all", Role::All), (" ALL ", Role::All)] {
            let cfg = DbConfig::from_env(env(&[("ROLE", raw)])).unwrap();
            assert_eq!(cfg.role, role, "{raw}");
        }
        let err = DbConfig::from_env(env(&[("ROLE", "writer")])).unwrap_err();
        assert!(
            format!("{err:#}").contains("all, web or indexer"),
            "{err:#}"
        );
    }

    /// One process owns a SQLite file; the split roles need a server.
    #[test]
    fn the_split_roles_need_postgres() {
        for role in ["web", "indexer"] {
            let err = DbConfig::from_env(env(&[("ROLE", role)])).unwrap_err();
            assert!(format!("{err:#}").contains("needs Postgres"), "{err:#}");
        }
    }
}
