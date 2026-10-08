//! Chain-derived writes, all on the writer session.

use anyhow::Result;
use sqlx::PgConnection;

use super::q::{self, PgQuery};
use super::shared::{hex_blob, without_nul};
use super::writer::{timed, Budget, TxFuture};
use super::{DbError, PgDb};
use crate::db::now_ts;
use crate::models::{Block, Transaction};
use crate::tokens::TokenMeta;

fn to_anyhow(e: DbError) -> anyhow::Error {
    anyhow::Error::new(e)
}

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

async fn exec(c: &mut PgConnection, what: &str, query: PgQuery) -> Result<u64, DbError> {
    timed(what, q::exec(c, what, query)).await
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

/// The lease's heartbeat, on the writer session.
pub(crate) async fn keepalive(p: &PgDb) {
    if let Some(w) = &p.writer {
        w.keepalive().await;
    }
}
