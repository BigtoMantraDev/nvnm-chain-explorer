//! Chain-derived writes, all on the writer session.
//!
//! A batch of blocks is set-based: one statement per table rather than one
//! per row, at most 14 round trips per batch (BEGIN, 11 statements, the
//! `writer_seq` bump, COMMIT) against about 642 for the per-row path.
//! Statements with nothing to do are skipped.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use num_bigint::BigInt;
use sqlx::PgConnection;

use super::plan::{apply_deltas, BalanceChanges, BatchPlan};
use super::q::{exec, fetch_all, get, PgQuery};
use super::shared::{bigint, hex_blob, without_nul};
use super::writer::{Budget, TxFuture};
use super::{DbError, PgDb};
use crate::db::{now_ts, Holder};
use crate::models::{AnchoringEvent, Block, BlockBundle, Transaction};
use crate::tokens::TokenMeta;

fn to_anyhow(e: DbError) -> anyhow::Error {
    anyhow::Error::new(e)
}

/// The per-block counter semantics of the per-row writer: a block already
/// stored is not counted again, nor are the transactions it already holds.
const PROBE: &str = "SELECT COALESCE(SUM(GREATEST(u.n - (SELECT COUNT(*) FROM transactions t WHERE t.block_number = u.num), 0)), 0)::int8,
            (SELECT COUNT(*) FROM blocks WHERE number = ANY($1))
     FROM UNNEST($1::int8[], $2::int8[]) AS u(num, n)";

const UPSERT_BLOCKS: &str = "INSERT INTO blocks (number, hash, parent_hash, timestamp, timestamp_ms, gas_used, gas_limit,
                         base_fee, size, extra_data, epoch, view, proposer, miner, tx_count, created_at)
     SELECT * FROM UNNEST($1::int8[], $2::bytea[], $3::bytea[], $4::int8[], $5::int8[], $6::int8[], $7::int8[],
                          $8::text[], $9::int8[], $10::text[], $11::int8[], $12::int8[], $13::bytea[],
                          $14::bytea[], $15::int8[], $16::int8[])
     ON CONFLICT (number) DO UPDATE SET
         hash = excluded.hash, parent_hash = excluded.parent_hash, timestamp = excluded.timestamp,
         timestamp_ms = excluded.timestamp_ms, gas_used = excluded.gas_used, gas_limit = excluded.gas_limit,
         base_fee = excluded.base_fee, size = excluded.size, extra_data = excluded.extra_data,
         epoch = excluded.epoch, view = excluded.view, proposer = excluded.proposer,
         miner = excluded.miner, tx_count = excluded.tx_count";

/// A rewrite that carries nothing for a blob keeps what is stored: the trace a
/// page cached, the raw bytes a failed decode left out.
const UPSERT_TXS: &str = "INSERT INTO transactions (hash, block_number, position, from_addr, to_addr, status, gas_used,
                               base_fee, contract_address, fee_token, fee_amount, input, raw,
                               trace_data, receipt_data, timestamp, created_at)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::bytea[], $5::bytea[], $6::int8[], $7::int8[],
                          $8::text[], $9::bytea[], $10::bytea[], $11::text[], $12::text[], $13::bytea[],
                          $14::text[], $15::text[], $16::int8[], $17::int8[])
     ON CONFLICT (hash) DO UPDATE SET
         block_number = excluded.block_number, position = excluded.position,
         from_addr = excluded.from_addr, to_addr = excluded.to_addr, status = excluded.status,
         gas_used = excluded.gas_used, base_fee = excluded.base_fee,
         contract_address = excluded.contract_address, fee_token = excluded.fee_token,
         fee_amount = excluded.fee_amount, input = excluded.input,
         raw = COALESCE(excluded.raw, transactions.raw),
         trace_data = COALESCE(excluded.trace_data, transactions.trace_data),
         receipt_data = COALESCE(excluded.receipt_data, transactions.receipt_data),
         timestamp = excluded.timestamp";

/// Qualified: a bare `n` is ambiguous on Postgres.
const BUMP_COUNTERS: &str =
    "INSERT INTO counters (name, n) VALUES ('blocks', $1), ('transactions', $2)
     ON CONFLICT (name) DO UPDATE SET n = counters.n + excluded.n";

/// `RETURNING` gives exactly the rows the per-row writer reports as new.
const INSERT_TRANSFERS: &str = "INSERT INTO transfer_events (tx_hash, block_number, log_index, token_addr, from_addr, to_addr,
                                  amount, timestamp, created_at)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::bytea[], $5::bytea[], $6::bytea[],
                          $7::text[], $8::int8[], $9::int8[])
     ON CONFLICT (block_number, log_index) DO NOTHING RETURNING block_number, log_index";

const INSERT_ANCHORING: &str =
    "INSERT INTO anchoring_events (tx_hash, block_number, log_index, timestamp, event, registry_id,
                                   record_id, caller)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::int8[], $5::text[], $6::int8[],
                          $7::int8[], $8::bytea[])
     ON CONFLICT (block_number, log_index) DO NOTHING RETURNING 1";

/// A new row counts the holders its balances already give it, as
/// `upsert_token_meta` does.
const UPSERT_TOKENS: &str = "INSERT INTO token_metadata (address, name, symbol, decimals, currency, total_supply, logo_uri,
                                 holder_count, created_at, updated_at)
     SELECT u.a, u.n, u.s, u.d, u.c, u.t, '',
            (SELECT COUNT(*) FROM token_balances b WHERE b.token_addr = u.x AND b.balance NOT LIKE '-%'), $8, $8
     FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::int8[], $5::text[], $6::text[], $7::text[])
          AS u(a, n, s, d, c, t, x)
     ON CONFLICT (address) DO UPDATE SET name = excluded.name, symbol = excluded.symbol,
         decimals = excluded.decimals, currency = excluded.currency,
         total_supply = excluded.total_supply, updated_at = excluded.updated_at";

const READ_BALANCES: &str = "SELECT b.token_addr, b.holder_addr, b.balance FROM token_balances b
     JOIN UNNEST($1::text[], $2::text[]) AS u(t, h) ON b.token_addr = u.t AND b.holder_addr = u.h";

const UPSERT_BALANCES: &str =
    "INSERT INTO token_balances (token_addr, holder_addr, balance, updated_at)
     SELECT u.t, u.h, u.b, $4 FROM UNNEST($1::text[], $2::text[], $3::text[]) AS u(t, h, b)
     ON CONFLICT (token_addr, holder_addr) DO UPDATE SET
         balance = excluded.balance, updated_at = excluded.updated_at";

const DELETE_BALANCES: &str =
    "DELETE FROM token_balances b USING UNNEST($1::text[], $2::text[]) AS u(t, h)
     WHERE b.token_addr = u.t AND b.holder_addr = u.h";

const BUMP_HOLDERS: &str =
    "UPDATE token_metadata m SET holder_count = m.holder_count + u.by, updated_at = $3
     FROM UNNEST($1::bytea[], $2::int8[]) AS u(addr, by) WHERE m.address = u.addr";

const SET_KV: &str = "INSERT INTO kv (key, value, updated_at) VALUES ($1, $2, $3)
     ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at";

fn blocks_query(sql: &'static str, blocks: &[Block]) -> PgQuery {
    let col = |f: fn(&Block) -> i64| blocks.iter().map(f).collect::<Vec<i64>>();
    let bytes = |f: fn(&Block) -> &str| blocks.iter().map(|b| hex_blob(f(b))).collect::<Vec<_>>();
    let text = |f: fn(&Block) -> &str| blocks.iter().map(|b| without_nul(f(b))).collect::<Vec<_>>();
    sqlx::query(sql)
        .bind(col(|b| b.number))
        .bind(bytes(|b| &b.hash))
        .bind(bytes(|b| &b.parent_hash))
        .bind(col(|b| b.timestamp))
        .bind(col(|b| b.timestamp_ms))
        .bind(col(|b| b.gas_used))
        .bind(col(|b| b.gas_limit))
        .bind(text(|b| &b.base_fee))
        .bind(col(|b| b.size))
        .bind(text(|b| &b.extra_data))
        .bind(col(|b| b.epoch))
        .bind(col(|b| b.view))
        .bind(bytes(|b| &b.proposer))
        .bind(bytes(|b| &b.miner))
        .bind(col(|b| b.tx_count))
        .bind(col(|b| b.created_at))
}

fn txs_query(sql: &'static str, txs: &[Transaction]) -> PgQuery {
    let col = |f: fn(&Transaction) -> i64| txs.iter().map(f).collect::<Vec<i64>>();
    let opt_bytes = |f: fn(&Transaction) -> Option<&str>| {
        txs.iter()
            .map(|t| f(t).map(hex_blob))
            .collect::<Vec<Option<Vec<u8>>>>()
    };
    let opt_text = |f: fn(&Transaction) -> Option<&str>| {
        txs.iter()
            .map(|t| f(t).map(without_nul))
            .collect::<Vec<Option<String>>>()
    };
    sqlx::query(sql)
        .bind(txs.iter().map(|t| hex_blob(&t.hash)).collect::<Vec<_>>())
        .bind(col(|t| t.block_number))
        .bind(col(|t| t.position))
        .bind(
            txs.iter()
                .map(|t| hex_blob(&t.from_addr))
                .collect::<Vec<_>>(),
        )
        .bind(opt_bytes(|t| t.to_addr.as_deref()))
        .bind(col(|t| t.status))
        .bind(col(|t| t.gas_used))
        .bind(
            txs.iter()
                .map(|t| without_nul(&t.base_fee))
                .collect::<Vec<_>>(),
        )
        .bind(opt_bytes(|t| t.contract_address.as_deref()))
        .bind(opt_bytes(|t| t.fee_token.as_deref()))
        .bind(
            txs.iter()
                .map(|t| without_nul(&t.fee_amount))
                .collect::<Vec<_>>(),
        )
        .bind(
            txs.iter()
                .map(|t| without_nul(&t.input))
                .collect::<Vec<_>>(),
        )
        .bind(opt_bytes(|t| t.raw.as_deref()))
        .bind(opt_text(|t| t.trace_data.as_deref()))
        .bind(opt_text(|t| t.receipt_data.as_deref()))
        .bind(col(|t| t.timestamp))
        .bind(col(|t| t.created_at))
}

fn tokens_query(tokens: &[TokenMeta], now: i64) -> PgQuery {
    sqlx::query(UPSERT_TOKENS)
        .bind(
            tokens
                .iter()
                .map(|m| hex_blob(&m.address))
                .collect::<Vec<_>>(),
        )
        .bind(
            tokens
                .iter()
                .map(|m| without_nul(&m.name))
                .collect::<Vec<_>>(),
        )
        .bind(
            tokens
                .iter()
                .map(|m| without_nul(&m.symbol))
                .collect::<Vec<_>>(),
        )
        .bind(tokens.iter().map(|m| m.decimals).collect::<Vec<_>>())
        .bind(
            tokens
                .iter()
                .map(|m| without_nul(&m.currency))
                .collect::<Vec<_>>(),
        )
        .bind(
            tokens
                .iter()
                .map(|m| without_nul(&m.total_supply))
                .collect::<Vec<_>>(),
        )
        .bind(tokens.iter().map(|m| m.address.clone()).collect::<Vec<_>>())
        .bind(now)
}

fn anchoring_query(events: &[AnchoringEvent]) -> PgQuery {
    sqlx::query(INSERT_ANCHORING)
        .bind(
            events
                .iter()
                .map(|e| hex_blob(&e.tx_hash))
                .collect::<Vec<_>>(),
        )
        .bind(events.iter().map(|e| e.block_number).collect::<Vec<_>>())
        .bind(events.iter().map(|e| e.log_index).collect::<Vec<_>>())
        .bind(events.iter().map(|e| e.timestamp).collect::<Vec<_>>())
        .bind(
            events
                .iter()
                .map(|e| without_nul(&e.event))
                .collect::<Vec<_>>(),
        )
        .bind(events.iter().map(|e| e.registry_id).collect::<Vec<_>>())
        .bind(events.iter().map(|e| e.record_id).collect::<Vec<_>>())
        .bind(
            events
                .iter()
                .map(|e| hex_blob(&e.caller))
                .collect::<Vec<_>>(),
        )
}

/// The stored balances of `keys`.
async fn read_balances(
    c: &mut PgConnection,
    keys: Vec<(String, String)>,
) -> Result<HashMap<(String, String), String>, DbError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (tokens, holders): (Vec<String>, Vec<String>) = keys.into_iter().unzip();
    let rows = fetch_all(
        c,
        "balances",
        sqlx::query(READ_BALANCES).bind(tokens).bind(holders),
    )
    .await?;
    let mut old = HashMap::with_capacity(rows.len());
    for row in &rows {
        old.insert(
            (get::<String>(row, 0)?, get::<String>(row, 1)?),
            get::<String>(row, 2)?,
        );
    }
    Ok(old)
}

/// Apply net balance deltas to the stored balances, and write the result.
async fn write_balances(
    c: &mut PgConnection,
    net: HashMap<(String, String), BigInt>,
) -> Result<(), DbError> {
    let old = read_balances(c, net.keys().cloned().collect()).await?;
    store_changes(c, apply_deltas(net, &old)).await
}

async fn store_changes(c: &mut PgConnection, out: BalanceChanges) -> Result<(), DbError> {
    let now = now_ts();
    if !out.upserts.is_empty() {
        let (mut t, mut h, mut b) = (Vec::new(), Vec::new(), Vec::new());
        for (token, holder, balance) in out.upserts {
            t.push(token);
            h.push(holder);
            b.push(balance);
        }
        exec(
            c,
            "upsert_bal",
            sqlx::query(UPSERT_BALANCES)
                .bind(t)
                .bind(h)
                .bind(b)
                .bind(now),
        )
        .await?;
    }
    if !out.deletes.is_empty() {
        let (t, h): (Vec<String>, Vec<String>) = out.deletes.into_iter().unzip();
        exec(
            c,
            "delete_bal",
            sqlx::query(DELETE_BALANCES).bind(t).bind(h),
        )
        .await?;
    }
    if !out.holders.is_empty() {
        let (t, by): (Vec<String>, Vec<i64>) = out.holders.into_iter().unzip();
        let t: Vec<Vec<u8>> = t.iter().map(|a| hex_blob(a)).collect();
        exec(
            c,
            "holders",
            sqlx::query(BUMP_HOLDERS).bind(t).bind(by).bind(now),
        )
        .await?;
    }
    Ok(())
}

/// One batch of blocks in one transaction on the writer session.
pub(crate) async fn save_block_bundles(p: &PgDb, bundles: &[BlockBundle]) -> Result<()> {
    if bundles.is_empty() {
        return Ok(());
    }
    let plan = Arc::new(BatchPlan::new(bundles));
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let plan = plan.clone();
            Box::pin(async move { write_plan(c, &plan).await })
        })
        .await
        .map_err(to_anyhow)
}

async fn write_plan(c: &mut PgConnection, plan: &BatchPlan) -> Result<(), DbError> {
    let (nums, counts) = plan.tx_counts();
    let probe = fetch_all(
        c,
        "probe",
        sqlx::query(PROBE).bind(nums.clone()).bind(counts),
    )
    .await?;
    let (new_txs, stored): (i64, i64) = (get(&probe[0], 0)?, get(&probe[0], 1)?);
    exec(c, "blocks", blocks_query(UPSERT_BLOCKS, plan.blocks())).await?;
    if !plan.txs().is_empty() {
        exec(c, "txs", txs_query(UPSERT_TXS, plan.txs())).await?;
    }
    exec(
        c,
        "counters",
        sqlx::query(BUMP_COUNTERS)
            .bind(plan.blocks().len() as i64 - stored)
            .bind(new_txs),
    )
    .await?;
    let mut fresh = HashSet::new();
    if !plan.transfers().is_empty() {
        let ts = plan.transfers();
        let query = sqlx::query(INSERT_TRANSFERS)
            .bind(ts.iter().map(|t| hex_blob(&t.tx_hash)).collect::<Vec<_>>())
            .bind(ts.iter().map(|t| t.block_number).collect::<Vec<_>>())
            .bind(ts.iter().map(|t| t.log_index).collect::<Vec<_>>())
            .bind(
                ts.iter()
                    .map(|t| hex_blob(&t.token_addr))
                    .collect::<Vec<_>>(),
            )
            .bind(
                ts.iter()
                    .map(|t| hex_blob(&t.from_addr))
                    .collect::<Vec<_>>(),
            )
            .bind(ts.iter().map(|t| hex_blob(&t.to_addr)).collect::<Vec<_>>())
            .bind(
                ts.iter()
                    .map(|t| without_nul(&t.amount))
                    .collect::<Vec<_>>(),
            )
            .bind(ts.iter().map(|t| t.timestamp).collect::<Vec<_>>())
            .bind(ts.iter().map(|t| t.created_at).collect::<Vec<_>>());
        for row in fetch_all(c, "transfers", query).await? {
            fresh.insert((get::<i64>(&row, 0)?, get::<i64>(&row, 1)?));
        }
    }
    if !plan.anchoring().is_empty() {
        exec(c, "anchoring", anchoring_query(plan.anchoring())).await?;
    }
    // Metadata before balances, so a new token's count is seeded first.
    if !plan.tokens().is_empty() {
        exec(c, "tokens", tokens_query(plan.tokens(), now_ts())).await?;
    }
    let keys = plan.balance_keys(&fresh);
    let old = read_balances(c, keys).await?;
    store_changes(c, plan.apply(&fresh, old)).await
}

pub(crate) async fn save_block(p: &PgDb, block: &Block) -> Result<()> {
    let block = block.clone();
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let block = block.clone();
            Box::pin(async move {
                exec(
                    c,
                    "save_block",
                    blocks_query(UPSERT_BLOCKS, std::slice::from_ref(&block)),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(to_anyhow)
}

pub(crate) async fn save_transaction(p: &PgDb, tx: &Transaction) -> Result<()> {
    let tx = tx.clone();
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let tx = tx.clone();
            Box::pin(async move {
                exec(
                    c,
                    "save_transaction",
                    txs_query(UPSERT_TXS, std::slice::from_ref(&tx)),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(to_anyhow)
}

pub(crate) async fn save_token_metadata(p: &PgDb, meta: &TokenMeta) -> Result<()> {
    let meta = meta.clone();
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let meta = meta.clone();
            Box::pin(async move {
                exec(
                    c,
                    "save_token_metadata",
                    tokens_query(std::slice::from_ref(&meta), now_ts()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(to_anyhow)
}

/// The indexer's view of the chain head, for the progress bar.
pub(crate) async fn set_chain_head(p: &PgDb, head: i64) {
    if let Err(e) = set_kv(p, "chain_head", &head.to_string()).await {
        tracing::warn!("set_chain_head: {e:#}");
    }
}

pub(crate) async fn set_kv(p: &PgDb, key: &str, value: &str) -> Result<()> {
    let (key, value) = (key.to_string(), value.to_string());
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let query = sqlx::query(SET_KV)
                .bind(key.clone())
                .bind(value.clone())
                .bind(now_ts());
            Box::pin(async move {
                exec(c, "set_kv", query).await?;
                Ok(())
            })
        })
        .await
        .map_err(to_anyhow)
}

/// Two passes, since the timestamps must be read inside the writer's
/// transaction: first ask `events` which blocks it needs (it returns early
/// on a missing stamp, so this pass is cheap), then read those blocks and
/// build the events for real. `events` must be deterministic.
pub(crate) async fn save_anchoring_window(
    p: &PgDb,
    key: &str,
    value: &str,
    events: impl Fn(&dyn Fn(i64) -> Option<i64>) -> Vec<AnchoringEvent> + Send + 'static,
) -> Result<usize> {
    let wanted = std::cell::RefCell::new(Vec::new());
    let _ = events(&|n| {
        wanted.borrow_mut().push(n);
        None
    });
    let mut numbers = wanted.into_inner();
    numbers.sort_unstable();
    numbers.dedup();
    let events = Arc::new(Mutex::new(events));
    let (key, value) = (key.to_string(), value.to_string());
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, usize> {
            let (events, numbers, key, value) =
                (events.clone(), numbers.clone(), key.clone(), value.clone());
            Box::pin(async move {
                let rows = fetch_all(
                    c,
                    "anchoring stamps",
                    sqlx::query("SELECT number, timestamp FROM blocks WHERE number = ANY($1)")
                        .bind(numbers),
                )
                .await?;
                let mut stamps = HashMap::with_capacity(rows.len());
                for row in &rows {
                    stamps.insert(get::<i64>(row, 0)?, get::<i64>(row, 1)?);
                }
                let built = {
                    let events = events.lock().unwrap_or_else(|e| e.into_inner());
                    events(&|n| stamps.get(&n).copied())
                };
                let built = dedup_events(built);
                let wrote = if built.is_empty() {
                    0
                } else {
                    fetch_all(c, "anchoring", anchoring_query(&built))
                        .await?
                        .len()
                };
                exec(
                    c,
                    "watermark",
                    sqlx::query(SET_KV).bind(key).bind(value).bind(now_ts()),
                )
                .await?;
                Ok(wrote)
            })
        })
        .await
        .map_err(to_anyhow)
}

/// First copy of each (block, log index), as `INSERT OR IGNORE` keeps it.
fn dedup_events(events: Vec<AnchoringEvent>) -> Vec<AnchoringEvent> {
    let mut seen = HashSet::new();
    events
        .into_iter()
        .filter(|e| seen.insert((e.block_number, e.log_index)))
        .collect()
}

/// Store the genesis balances not stored yet, add them to their holders',
/// and move the cursor, in one transaction.
pub(crate) async fn save_genesis_balances(
    p: &PgDb,
    balances: &[(Holder, String)],
    cursor: i64,
) -> Result<()> {
    let mut rows: Vec<(String, String, String)> = Vec::new();
    let mut seen = HashSet::new();
    for ((token, holder), balance) in balances {
        if seen.insert((token.clone(), holder.clone())) {
            rows.push((token.clone(), holder.clone(), balance.clone()));
        }
    }
    let rows = Arc::new(rows);
    p.writer()
        .map_err(to_anyhow)?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let rows = rows.clone();
            Box::pin(async move {
                let mut net: HashMap<(String, String), BigInt> = HashMap::new();
                if !rows.is_empty() {
                    let inserted = fetch_all(
                        c,
                        "genesis",
                        sqlx::query(
                            "INSERT INTO genesis_balances (token_addr, holder_addr, balance) \
                             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[]) \
                             ON CONFLICT (token_addr, holder_addr) DO NOTHING \
                             RETURNING token_addr, holder_addr, balance",
                        )
                        .bind(rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>())
                        .bind(rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>())
                        .bind(rows.iter().map(|r| r.2.clone()).collect::<Vec<_>>()),
                    )
                    .await?;
                    for row in &inserted {
                        let key = (get::<String>(row, 0)?, get::<String>(row, 1)?);
                        *net.entry(key).or_default() += bigint(&get::<String>(row, 2)?);
                    }
                }
                write_balances(c, net).await?;
                exec(
                    c,
                    "genesis cursor",
                    sqlx::query(SET_KV)
                        .bind("genesis_balances_cursor")
                        .bind(cursor.to_string())
                        .bind(now_ts()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(to_anyhow)
}

/// The lease's heartbeat, on the writer session.
pub(crate) async fn keepalive(p: &PgDb) {
    if let Some(w) = &p.writer {
        w.keepalive().await;
    }
}
