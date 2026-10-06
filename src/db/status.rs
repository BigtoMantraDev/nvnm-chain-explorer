//! What this process reports about its database, for `/readyz`.
//!
//! The database layer publishes it on a `watch` channel, so a probe reads the
//! latest value and never waits on, or takes, a database connection.

use serde::Serialize;

use super::migrations;
use super::Role;

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub role: Role,
    pub schema: SchemaVersions,
}

/// D, the database's version (unknown until it has been read), and B, this
/// binary's.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SchemaVersions {
    pub db: Option<i64>,
    pub binary: i64,
}

impl Status {
    /// Before the database has been opened.
    pub fn starting(role: Role) -> Self {
        Status {
            role,
            schema: SchemaVersions {
                db: None,
                binary: migrations::binary_version(),
            },
        }
    }

    /// Whether this process should get traffic. Database health never
    /// counts: a failed read is answered where it happens.
    pub fn ready(&self) -> bool {
        self.schema.db.is_some_and(migrations::web_ready)
    }
}
