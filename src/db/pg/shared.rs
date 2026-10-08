//! What the Postgres backend shares with `sqlite.rs`: copies of its small
//! helpers, so their visibility there does not change, and the column lists as
//! macros, because sqlx takes only a `&'static str` and `concat!` needs
//! literals. `sqlite/migrate.rs` tests each list against its SQLite constant.

/// Decode a `0x`-prefixed hex string into raw bytes, as `sqlite.rs` stores
/// hashes and addresses; a non-hex value is stored as its own bytes.
pub(crate) fn hex_blob(s: &str) -> Vec<u8> {
    let hexed = s.strip_prefix("0x").unwrap_or(s);
    match hex::decode(hexed) {
        Ok(b) => b,
        Err(_) => s.as_bytes().to_vec(),
    }
}

pub(crate) fn blob_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// One stored address, in the checksummed form the rest of the explorer keys on.
pub(crate) fn blob_addr(bytes: &[u8]) -> String {
    crate::decoder::checksum_address(&blob_hex(bytes))
}

pub(crate) fn bigint(s: &str) -> num_bigint::BigInt {
    num_bigint::BigInt::parse_bytes(s.as_bytes(), 10).unwrap_or_else(|| num_bigint::BigInt::from(0))
}

/// Postgres `TEXT` refuses a NUL byte, which SQLite stores. Caches drop it.
pub(crate) fn without_nul(s: &str) -> String {
    s.replace('\0', "")
}

macro_rules! block_cols {
    () => {
        "number, hash, parent_hash, timestamp, timestamp_ms, gas_used, gas_limit, base_fee, size, \
         extra_data, epoch, view, proposer, miner, tx_count, created_at"
    };
}

macro_rules! tx_cols {
    () => {
        "hash, block_number, position, from_addr, to_addr, status, gas_used, base_fee, contract_address, fee_token, fee_amount, input, raw, trace_data, receipt_data, timestamp, created_at"
    };
}

macro_rules! tx_list_cols {
    () => {
        "hash, block_number, position, from_addr, to_addr, status, gas_used, base_fee, contract_address, fee_token, fee_amount, input, NULL, NULL, NULL, timestamp, created_at"
    };
}

macro_rules! token_cols {
    () => {
        "address, name, symbol, decimals, currency, total_supply, logo_uri, \
                          holder_count, created_at, updated_at"
    };
}

macro_rules! transfer_cols {
    () => {
        "e.id, e.tx_hash, e.block_number, e.log_index, e.token_addr, \
                             e.from_addr, e.to_addr, e.amount, e.timestamp, e.created_at"
    };
}

/// What a holders listing counts and shows: `HOLDING` in `sqlite.rs`, as a
/// literal for `concat!`, and `idx_tb_holding`'s predicate.
macro_rules! holding {
    () => {
        "balance NOT LIKE '-%'"
    };
}

pub(crate) use {block_cols, holding, token_cols, transfer_cols, tx_cols, tx_list_cols};
