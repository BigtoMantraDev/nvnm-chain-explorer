# Coding conventions

Rust project. CI (`.github/workflows/ci.yml`) enforces the baseline:

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`

## Errors

Application code returns `anyhow::Result` and uses `anyhow::Context` to attach
context when propagating errors (see `src/main.rs`). Never swallow errors —
log them with `tracing` or propagate them.

## Tests

- No network: `cargo test --lib --test decoder --test anchoring --test pages --test health --test migrations --test shutdown`
- Against the live chain RPC: `cargo test --test live_rpc --test baseline`
- Against Postgres: `docker compose up -d --wait`, then `PG_TEST_URL=postgres://explorer:explorer@localhost:5432/explorer cargo test --test postgres -- --include-ignored`

`docs/database.md` has the details, and how to add a schema change.
