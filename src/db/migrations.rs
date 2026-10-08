//! The schema versions both backends share, and the rules their files follow.
//!
//! Version 1 is the baseline: on SQLite it is `init_db` itself, with no file;
//! on Postgres it is `migrations/postgres/0001_baseline.sql`. From 0002 on,
//! every version is a twin pair, `migrations/sqlite/NNNN_name.sql` and
//! `migrations/postgres/NNNN_name.sql`, with the same number and name.
//! Version numbers are assigned on NVNM-Chain's `main` only (docs/database.md).

use std::collections::HashSet;
use std::sync::LazyLock;

use sha3::{Digest, Sha3_256};

/// One schema version. `sqlite` is `None` for version 1, which is `init_db`.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sqlite: Option<&'static str>,
    pub postgres: &'static str,
}

/// `NNNN => "name";`, one line per version, in order. The number is written
/// zero-padded because it is also the file name's prefix.
macro_rules! migrations {
    ($v1:literal => $n1:literal; $($v:literal => $n:literal;)*) => {
        #[allow(clippy::zero_prefixed_literal)]
        pub static MIGRATIONS: &[Migration] = &[
            Migration {
                version: $v1,
                name: $n1,
                sqlite: None,
                postgres: include_str!(concat!(
                    "../../migrations/postgres/", stringify!($v1), "_", $n1, ".sql"
                )),
            },
            $(Migration {
                version: $v,
                name: $n,
                sqlite: Some(include_str!(concat!(
                    "../../migrations/sqlite/", stringify!($v), "_", $n, ".sql"
                ))),
                postgres: include_str!(concat!(
                    "../../migrations/postgres/", stringify!($v), "_", $n, ".sql"
                )),
            },)*
        ];
    };
}

migrations! {
    0001 => "baseline";
}

/// The first version a database's rows skip, and the version found in its
/// place. The runners record versions in order, so a gap means hand edits.
pub fn first_gap(applied: impl IntoIterator<Item = i64>) -> Option<(i64, i64)> {
    applied
        .into_iter()
        .zip(1..)
        .find(|(have, want)| have != want)
        .map(|(have, want)| (want, have))
}

/// B: the newest version this binary knows.
pub fn binary_version() -> i64 {
    MIGRATIONS.last().map_or(0, |m| m.version)
}

/// sha3-256 of a file's text, hex, with CRLF read as LF so a checkout's line
/// endings never look like an edit.
pub fn checksum(text: &str) -> String {
    hex::encode(Sha3_256::digest(text.replace("\r\n", "\n").as_bytes()))
}

/// Each version's Postgres checksum, computed once per process.
pub fn postgres_checksums() -> &'static [String] {
    static SUMS: LazyLock<Vec<String>> =
        LazyLock::new(|| MIGRATIONS.iter().map(|m| checksum(m.postgres)).collect());
    &SUMS
}

/// Whether a web replica may serve a database at version `db`: once the
/// indexer has applied every migration this binary knows.
pub fn web_ready(db: i64) -> bool {
    db >= binary_version()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Expand,
    Contract,
}

/// What a file's leading `-- ` header lines say.
#[derive(Debug, Default)]
pub struct Headers {
    pub kind: Option<Kind>,
    pub no_transaction: bool,
    /// `sqlite-only` or `postgres-only`: the backend that has the change.
    pub noop: Option<String>,
}

/// Read the header block: the `-- key: value` and `-- flag` lines before the
/// first line that is neither a comment nor blank. Other comments are prose,
/// a `-- kind:` line further down included: the block must name the kind.
pub fn headers(text: &str) -> (Headers, Vec<String>) {
    let mut h = Headers::default();
    let mut errors = Vec::new();
    let mut kind_named = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix("--") else {
            break;
        };
        let rest = rest.trim();
        match rest.split_once(':') {
            Some(("kind", v)) => {
                kind_named = true;
                match v.trim() {
                    "expand" => h.kind = Some(Kind::Expand),
                    "contract" => h.kind = Some(Kind::Contract),
                    other => errors.push(format!("unknown kind `{other}`")),
                }
            }
            Some(("noop", v)) => h.noop = Some(v.trim().to_string()),
            _ if rest == "no-transaction" => h.no_transaction = true,
            _ if looks_like_header(rest) => errors.push(format!("unknown header `-- {rest}`")),
            _ => {}
        }
    }
    if !kind_named {
        errors.push("no `-- kind:` header".into());
    }
    (h, errors)
}

/// A comment that reads like a misspelt header rather than prose: one word,
/// or `word: value`, with no spaces before the colon.
fn looks_like_header(rest: &str) -> bool {
    let head = rest.split(':').next().unwrap_or("");
    !head.is_empty()
        && !head.contains(' ')
        && head.chars().all(|c| c.is_ascii_lowercase() || c == '-')
}

/// The statements of a file: `--` comment lines dropped, split at `;`.
pub fn statements(text: &str) -> Vec<String> {
    let code: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    code.split(';')
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
        .collect()
}

/// The queries a `-- no-transaction` file is run as: split at a `;` ending a
/// line, comment lines left out. The runner sends each on its own, so each
/// must be one statement (`no_transaction_errors`).
pub fn split_at_line_end(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }
        current.push_str(line);
        current.push('\n');
        if line.trim_end().ends_with(';') {
            let stmt = current.trim().trim_end_matches(';').trim().to_string();
            if !stmt.is_empty() {
                out.push(stmt);
            }
            current.clear();
        }
    }
    let rest = current.trim();
    if !rest.is_empty() {
        out.push(rest.to_string());
    }
    out
}

/// Whether `stmt` contains the keyword sequence `words`, as whole words and
/// ignoring case.
fn has(stmt: &str, words: &str) -> bool {
    let toks: Vec<String> = tokens(stmt);
    let want: Vec<&str> = words.split(' ').collect();
    toks.windows(want.len())
        .any(|w| w.iter().zip(&want).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

fn tokens(stmt: &str) -> Vec<String> {
    stmt.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// The word after the keyword sequence `words`, skipping `IF [NOT] EXISTS`
/// and `CONCURRENTLY`: the object a statement names.
fn object_after(stmt: &str, words: &str) -> Option<String> {
    let toks = tokens(stmt);
    let want: Vec<&str> = words.split(' ').collect();
    let at = toks
        .windows(want.len())
        .position(|w| w.iter().zip(&want).all(|(a, b)| a.eq_ignore_ascii_case(b)))?;
    toks[at + want.len()..]
        .iter()
        .find(|t| {
            !["IF", "NOT", "EXISTS", "CONCURRENTLY", "ONLY"]
                .iter()
                .any(|k| t.eq_ignore_ascii_case(k))
        })
        .map(|t| t.to_lowercase())
}

/// The index a `CREATE [UNIQUE] INDEX` statement makes, and its table.
fn created_index(stmt: &str) -> Option<(String, String)> {
    if !(has(stmt, "CREATE INDEX") || has(stmt, "CREATE UNIQUE INDEX")) {
        return None;
    }
    let name = object_after(stmt, "INDEX")?;
    let table = object_after(stmt, "ON")?;
    Some((name, table))
}

/// The index names a `-- no-transaction` file builds with `CREATE … INDEX
/// CONCURRENTLY`: what the preflight lets be INVALID until the file has run.
pub fn concurrent_indexes(text: &str) -> Vec<String> {
    statements(text)
        .iter()
        .filter(|s| has(s, "CONCURRENTLY"))
        .filter_map(|s| created_index(s).map(|(name, _)| name))
        .collect()
}

const EXPAND_FORBIDS: &[&str] = &[
    "DROP TABLE",
    "RENAME",
    "SET NOT NULL",
    "TRUNCATE",
    "DELETE FROM",
];
const CONSTRAINTS: &[&str] = &["ADD CONSTRAINT", "UNIQUE", "CHECK", "REFERENCES"];
/// Too big to lock while an index builds.
const BIG_TABLES: &[&str] = &["transactions", "transfer_events"];

/// Every rule one version's twin pair breaks; empty when it is well formed.
pub fn check_pair(version: i64, name: &str, sqlite: &str, postgres: &str) -> Vec<String> {
    let at = |side: &str, msg: String| format!("{version:04}_{name} {side}: {msg}");
    let mut errors = Vec::new();
    let (sh, se) = headers(sqlite);
    let (ph, pe) = headers(postgres);
    errors.extend(se.into_iter().map(|e| at("sqlite", e)));
    errors.extend(pe.into_iter().map(|e| at("postgres", e)));
    if let (Some(a), Some(b)) = (sh.kind, ph.kind) {
        if a != b {
            errors.push(at(
                "both",
                format!("kinds differ: sqlite {a:?}, postgres {b:?}"),
            ));
        }
    }
    if sh.no_transaction {
        errors.push(at("sqlite", "`-- no-transaction` is Postgres only".into()));
    }
    for (side, h, text, other) in [
        ("sqlite", &sh, sqlite, "postgres-only"),
        ("postgres", &ph, postgres, "sqlite-only"),
    ] {
        if let Some(noop) = &h.noop {
            if noop != other {
                errors.push(at(
                    side,
                    format!("noop must name the other backend: `-- noop: {other}`"),
                ));
            }
            if !statements(text).is_empty() {
                errors.push(at(side, "a noop file has no statements".into()));
            }
        }
    }
    if sh.noop.is_some() && ph.noop.is_some() {
        errors.push(at("both", "both twins are noop".into()));
    }
    let kind = ph.kind.or(sh.kind);
    for (side, text) in [("sqlite", sqlite), ("postgres", postgres)] {
        if kind == Some(Kind::Expand) {
            errors.extend(expand_errors(text).into_iter().map(|e| at(side, e)));
        }
    }
    if ph.no_transaction {
        errors.extend(
            no_transaction_errors(postgres)
                .into_iter()
                .map(|e| at("postgres", e)),
        );
    } else {
        errors.extend(
            transactional_pg_errors(postgres)
                .into_iter()
                .map(|e| at("postgres", e)),
        );
    }
    errors
}

fn expand_errors(text: &str) -> Vec<String> {
    let stmts = statements(text);
    let mut errors = Vec::new();
    let created: HashSet<String> = stmts
        .iter()
        .filter(|s| has(s, "CREATE TABLE"))
        .filter_map(|s| object_after(s, "TABLE"))
        .collect();
    for (i, s) in stmts.iter().enumerate() {
        for f in EXPAND_FORBIDS {
            if has(s, f) {
                errors.push(format!("an expand may not {f}; label it `contract`: {s}"));
            }
        }
        if alters_existing(s) {
            errors.push(format!(
                "an expand may not drop or alter what a table has; label it `contract`: {s}"
            ));
        }
        if CONSTRAINTS.iter().any(|c| has(s, c)) {
            let own = if has(s, "CREATE TABLE") {
                true
            } else if let Some((_, table)) = created_index(s) {
                created.contains(&table)
            } else {
                false
            };
            if !own {
                errors.push(format!(
                    "an expand adds constraints only on a table this file creates: {s}"
                ));
            }
        }
        if has(s, "DROP INDEX") {
            let Some(x) = object_after(s, "INDEX") else {
                continue;
            };
            let rebuilt = stmts[i + 1..]
                .iter()
                .any(|later| created_index(later).is_some_and(|(n, _)| n == x));
            if !rebuilt {
                errors.push(format!("an expand drops index {x} without re-creating it"));
            }
        }
    }
    errors
}

/// Whether an `ALTER TABLE` has a `DROP` or `ALTER` action, which both
/// engines accept with or without `COLUMN`.
fn alters_existing(stmt: &str) -> bool {
    let toks = tokens(stmt);
    let Some(at) = toks
        .windows(2)
        .position(|w| w[0].eq_ignore_ascii_case("ALTER") && w[1].eq_ignore_ascii_case("TABLE"))
    else {
        return false;
    };
    toks[at + 2..]
        .iter()
        .any(|t| t.eq_ignore_ascii_case("DROP") || t.eq_ignore_ascii_case("ALTER"))
}

fn no_transaction_errors(text: &str) -> Vec<String> {
    let mut errors = Vec::new();
    if text.contains("$$") {
        errors.push("no `$$` bodies: statements are split at `;`".into());
    }
    for query in split_at_line_end(text) {
        if statements(&query).len() > 1 {
            errors.push(format!(
                "end a statement's last line with its `;`, nothing after it: the runner \
                 sends what lies between as one query, which `CONCURRENTLY` refuses: {query}"
            ));
        }
    }
    let mut dropped = HashSet::new();
    for s in statements(text) {
        let drop = has(&s, "DROP INDEX CONCURRENTLY IF EXISTS");
        let create = (has(&s, "CREATE INDEX CONCURRENTLY")
            || has(&s, "CREATE UNIQUE INDEX CONCURRENTLY"))
            && created_index(&s).is_some();
        if drop {
            if let Some(x) = object_after(&s, "INDEX") {
                dropped.insert(x);
            }
        } else if create {
            let (x, _) = created_index(&s).expect("checked above");
            if has(&s, "IF NOT EXISTS") {
                errors.push(format!(
                    "{x}: never `IF NOT EXISTS` here; IF NOT EXISTS would hide an INVALID index"
                ));
            }
            if !dropped.contains(&x) {
                errors.push(format!(
                    "CREATE … CONCURRENTLY {x} must follow DROP INDEX CONCURRENTLY IF EXISTS {x}"
                ));
            }
        } else {
            errors.push(format!(
                "a no-transaction file holds only DROP INDEX CONCURRENTLY IF EXISTS and \
                 CREATE [UNIQUE] INDEX CONCURRENTLY: {s}"
            ));
        }
    }
    errors
}

fn transactional_pg_errors(text: &str) -> Vec<String> {
    let mut errors = Vec::new();
    for s in statements(text) {
        if has(&s, "CONCURRENTLY") {
            errors.push(format!(
                "CONCURRENTLY needs a `-- no-transaction` file: {s}"
            ));
        } else if has(&s, "DROP INDEX") {
            errors.push(format!(
                "drop an index with DROP INDEX CONCURRENTLY in a `-- no-transaction` file: {s}"
            ));
        } else if let Some((x, table)) = created_index(&s) {
            if BIG_TABLES.contains(&table.as_str()) {
                errors.push(format!(
                    "{x} on {table}: build it with CREATE INDEX CONCURRENTLY in a \
                     `-- no-transaction` file"
                ));
            }
        }
    }
    errors
}

/// Every rule the list breaks: versions run 1, 2, 3… with no gap or repeat,
/// version 1 has no SQLite file and every later version has both, and each
/// pair is well formed.
pub fn check_list(list: &[Migration]) -> Vec<String> {
    let mut errors = Vec::new();
    for (i, m) in list.iter().enumerate() {
        let want = i as i64 + 1;
        if m.version != want {
            errors.push(format!(
                "{:04}_{}: expected version {want:04}; versions run 1, 2, 3… with no gap or repeat",
                m.version, m.name
            ));
        }
        match (m.version, m.sqlite) {
            (1, None) => {}
            (1, Some(_)) => errors.push("0001: on SQLite, version 1 is init_db".into()),
            (_, None) => errors.push(format!("{:04}_{}: no SQLite twin", m.version, m.name)),
            (v, Some(sqlite)) => errors.extend(check_pair(v, m.name, sqlite, m.postgres)),
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_gap_names_the_first_skipped_version() {
        assert_eq!(first_gap([]), None);
        assert_eq!(first_gap([1, 2, 3]), None);
        assert_eq!(first_gap([1, 3]), Some((2, 3)));
        assert_eq!(first_gap([2]), Some((1, 2)));
        assert_eq!(first_gap([1, 2, 4, 5]), Some((3, 4)));
    }

    const EXPAND: &str = "-- kind: expand\n";

    fn errors(sqlite: &str, pg: &str) -> Vec<String> {
        check_pair(2, "change", sqlite, pg)
    }

    fn assert_refused(sqlite: &str, pg: &str, needle: &str) {
        let found = errors(sqlite, pg);
        assert!(
            found.iter().any(|e| e.contains(needle)),
            "expected an error containing {needle:?}, got {found:?}"
        );
    }

    #[test]
    fn a_new_table_is_a_well_formed_expand() {
        let sqlite = format!("{EXPAND}CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE);\n");
        let pg = format!("{EXPAND}CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT UNIQUE);\n");
        assert_eq!(errors(&sqlite, &pg), Vec::<String>::new());
    }

    #[test]
    fn every_file_names_its_kind() {
        assert_refused(
            "CREATE TABLE t (id INTEGER);",
            EXPAND,
            "sqlite: no `-- kind:` header",
        );
        assert_refused(EXPAND, "-- kind: sideways\n", "postgres: unknown kind");
    }

    #[test]
    fn a_kind_after_the_header_block_does_not_count() {
        let late = "DROP TABLE blocks;\n-- kind: expand\n";
        assert_refused(late, late, "sqlite: no `-- kind:` header");
        assert_refused(late, late, "postgres: no `-- kind:` header");
    }

    #[test]
    fn twins_agree_on_their_kind() {
        assert_refused(EXPAND, "-- kind: contract\nDROP TABLE t;", "kinds differ");
    }

    #[test]
    fn an_unknown_header_is_refused() {
        assert_refused(
            &format!("{EXPAND}-- no-trasaction\n"),
            EXPAND,
            "unknown header",
        );
    }

    #[test]
    fn an_expand_cannot_remove_or_rewrite() {
        for body in [
            "DROP TABLE t;",
            "ALTER TABLE t DROP COLUMN c;",
            "ALTER TABLE t DROP c;",
            "ALTER TABLE t ADD COLUMN d TEXT, DROP CONSTRAINT k;",
            "ALTER TABLE t RENAME TO u;",
            "ALTER TABLE t ALTER COLUMN c SET NOT NULL;",
            "ALTER TABLE t ALTER c TYPE TEXT;",
            "TRUNCATE t;",
            "DELETE FROM kv WHERE key = 'x';",
        ] {
            let pg = format!("{EXPAND}{body}\n");
            let found = errors(EXPAND, &pg);
            assert!(
                found.iter().any(|e| e.contains("expand")),
                "{body}: {found:?}"
            );
        }
    }

    #[test]
    fn the_expand_check_matches_whole_keywords_and_skips_comments() {
        let pg = format!(
            "{EXPAND}-- this used to DROP TABLE things\nCREATE TABLE renamed_things (truncated_at BIGINT);\n"
        );
        assert_eq!(errors(EXPAND, &pg), Vec::<String>::new());
    }

    #[test]
    fn an_expand_constrains_only_tables_it_creates() {
        let pg = format!("{EXPAND}CREATE UNIQUE INDEX ux ON blocks (hash);\n");
        assert_refused(EXPAND, &pg, "only on a table this file creates");
        let pg = format!("{EXPAND}ALTER TABLE blocks ADD CONSTRAINT c CHECK (number > 0);\n");
        assert_refused(EXPAND, &pg, "only on a table this file creates");
        let ok = format!(
            "{EXPAND}CREATE TABLE fresh (a BIGINT, b BIGINT REFERENCES blocks (number));\n\
             CREATE UNIQUE INDEX ux_fresh ON fresh (a);\n"
        );
        assert_eq!(errors(EXPAND, &ok), Vec::<String>::new());
    }

    #[test]
    fn an_expand_drops_an_index_only_to_rebuild_it() {
        let pg = "-- kind: expand\n-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS x_v2;\n";
        assert_refused(EXPAND, pg, "drops index x_v2 without re-creating it");
        let pg = "-- kind: expand\n-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS x_v2;\n\
                  CREATE INDEX CONCURRENTLY x_v2 ON blocks (miner);\n";
        assert_eq!(errors(EXPAND, pg), Vec::<String>::new());
    }

    #[test]
    fn no_transaction_is_for_postgres_only() {
        assert_refused(
            "-- kind: expand\n-- no-transaction\n",
            EXPAND,
            "Postgres only",
        );
    }

    #[test]
    fn a_no_transaction_file_holds_only_concurrent_index_changes() {
        let pg = "-- kind: expand\n-- no-transaction\nCREATE TABLE t (a BIGINT);\n";
        assert_refused(EXPAND, pg, "only DROP INDEX CONCURRENTLY IF EXISTS");
        let pg =
            "-- kind: expand\n-- no-transaction\nCREATE INDEX CONCURRENTLY x ON blocks (miner);\n";
        assert_refused(
            EXPAND,
            pg,
            "must follow DROP INDEX CONCURRENTLY IF EXISTS x",
        );
        let pg = "-- kind: expand\n-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS x;\n\
                  CREATE INDEX CONCURRENTLY IF NOT EXISTS x ON blocks (miner);\n";
        assert_refused(EXPAND, pg, "IF NOT EXISTS would hide an INVALID index");
    }

    #[test]
    fn a_no_transaction_file_ends_each_statement_at_a_line_end() {
        let one_line = "-- kind: expand\n-- no-transaction\n\
                        DROP INDEX CONCURRENTLY IF EXISTS x; CREATE INDEX CONCURRENTLY x ON blocks (miner);\n";
        assert_refused(EXPAND, one_line, "nothing after it");
        let noted = "-- kind: expand\n-- no-transaction\n\
                     DROP INDEX CONCURRENTLY IF EXISTS x; -- the old one\n\
                     CREATE INDEX CONCURRENTLY x ON blocks (miner);\n";
        assert_refused(EXPAND, noted, "nothing after it");
        let split = "-- kind: expand\n-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS x;\n\
                     CREATE INDEX CONCURRENTLY x -- by miner\n    ON blocks (miner);\n";
        assert_eq!(errors(EXPAND, split), Vec::<String>::new());
    }

    #[test]
    fn statements_split_at_a_semicolon_ending_a_line() {
        let text = "-- kind: expand\n-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS x;\n\
                    CREATE INDEX CONCURRENTLY x\n    ON blocks (miner);\n";
        assert_eq!(
            split_at_line_end(text),
            [
                "DROP INDEX CONCURRENTLY IF EXISTS x",
                "CREATE INDEX CONCURRENTLY x\n    ON blocks (miner)"
            ]
        );
    }

    #[test]
    fn a_transactional_postgres_file_never_locks_the_big_tables() {
        let pg = format!("{EXPAND}CREATE INDEX x ON transactions (fee_token);\n");
        assert_refused(EXPAND, &pg, "CONCURRENTLY");
        let pg = format!("{EXPAND}CREATE INDEX x ON transfer_events (amount);\n");
        assert_refused(EXPAND, &pg, "CONCURRENTLY");
        let pg = "-- kind: contract\nDROP INDEX x;\n";
        assert_refused("-- kind: contract\n", pg, "DROP INDEX CONCURRENTLY");
        let pg = format!("{EXPAND}CREATE INDEX x ON blocks (miner);\n");
        assert_eq!(errors(EXPAND, &pg), Vec::<String>::new());
    }

    #[test]
    fn a_noop_twin_is_empty_and_names_the_other_backend() {
        let sqlite = "-- kind: expand\n-- noop: postgres-only\n";
        let pg = format!("{EXPAND}CREATE TABLE t (a BIGINT);\n");
        assert_eq!(errors(sqlite, &pg), Vec::<String>::new());
        assert_refused(
            "-- kind: expand\n-- noop: postgres-only\nCREATE TABLE t (a INTEGER);\n",
            &pg,
            "a noop file has no statements",
        );
        assert_refused(
            "-- kind: expand\n-- noop: sqlite-only\n",
            &pg,
            "noop must name",
        );
    }

    #[test]
    fn the_list_is_contiguous_from_one() {
        let list = [entry(1, "baseline"), entry(3, "skipped")];
        let found = check_list(&list);
        assert!(found.iter().any(|e| e.contains("0003")), "{found:?}");
        let list = [entry(1, "baseline"), entry(2, "a"), entry(2, "b")];
        let found = check_list(&list);
        assert!(found.iter().any(|e| e.contains("0002")), "{found:?}");
    }

    fn entry(version: i64, name: &'static str) -> Migration {
        Migration {
            version,
            name,
            sqlite: (version > 1).then_some("-- kind: expand\n"),
            postgres: "-- kind: expand\n",
        }
    }

    /// The real list: every version well formed, and every file on disk in it.
    #[test]
    fn the_shipped_migrations_follow_the_rules() {
        assert_eq!(check_list(MIGRATIONS), Vec::<String>::new());
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        for dialect in ["sqlite", "postgres"] {
            let Ok(dir) = std::fs::read_dir(root.join(dialect)) else {
                continue;
            };
            for file in dir {
                let name = file.unwrap().file_name().into_string().unwrap();
                let listed = MIGRATIONS.iter().any(|m| {
                    name == format!("{:04}_{}.sql", m.version, m.name)
                        && (dialect == "postgres" || m.version > 1)
                });
                assert!(
                    listed,
                    "migrations/{dialect}/{name} is not in migrations!{{}}"
                );
            }
        }
    }

    #[test]
    fn a_checksum_ignores_line_endings() {
        assert_eq!(checksum("a\r\nb\r\n"), checksum("a\nb\n"));
        assert_ne!(checksum("a\nb\n"), checksum("a\nc\n"));
        assert_eq!(checksum("").len(), 64);
    }

    #[test]
    fn web_is_ready_once_the_database_has_caught_up() {
        let b = binary_version();
        assert!(!web_ready(b - 1));
        assert!(web_ready(b));
        assert!(web_ready(b + 1), "an older web replica keeps serving");
    }
}
