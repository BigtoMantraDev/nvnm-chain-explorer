//! What the indexer leans on in the database under `ROLE=indexer`.
//!
//! Needs a Postgres server: ignored unless run with `--include-ignored`, and
//! then failing without `PG_TEST_URL`.

use nvnmchain_explorer::db::{self, Db, Role, Status};
use nvnmchain_explorer::decoder::checksum_address;
use nvnmchain_explorer::models::{Block, BlockBundle, Transaction, TransferEvent};
use nvnmchain_explorer::tokens::TokenMeta;

#[path = "common/backend.rs"]
mod backend;

async fn open(url: &str, role: Role) -> Db {
    db::open_with(
        &backend::pg_config(url, role),
        tokio::sync::watch::channel(Status::starting(role)).0,
    )
    .await
    .unwrap()
}

fn bundle(number: i64, token: &str, fee_token: &str) -> BlockBundle {
    let tx = Transaction {
        hash: format!("0x{:064x}", number + 1000),
        block_number: number,
        position: 0,
        from_addr: checksum_address("0x1111111111111111111111111111111111111111"),
        to_addr: None,
        status: 1,
        gas_used: 0,
        base_fee: "0".into(),
        contract_address: None,
        fee_token: Some(checksum_address(fee_token)),
        fee_amount: "1".into(),
        input: "0x".into(),
        raw: None,
        trace_data: None,
        receipt_data: None,
        timestamp: 0,
        created_at: 0,
    };
    BlockBundle {
        block: Block {
            number,
            hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number - 1),
            timestamp: 0,
            timestamp_ms: 0,
            gas_used: 0,
            gas_limit: 0,
            base_fee: "0".into(),
            size: 0,
            extra_data: String::new(),
            epoch: 0,
            view: 0,
            proposer: format!("0x{}", "33".repeat(20)),
            miner: format!("0x{}", "33".repeat(20)),
            tx_count: 1,
            created_at: 0,
        },
        transfers: vec![TransferEvent {
            id: 0,
            tx_hash: tx.hash.clone(),
            block_number: number,
            log_index: 0,
            token_addr: checksum_address(token),
            from_addr: checksum_address("0x1111111111111111111111111111111111111111"),
            to_addr: checksum_address("0x2222222222222222222222222222222222222222"),
            amount: "5".into(),
            timestamp: 0,
            created_at: 0,
        }],
        txs: vec![tx],
        anchoring: Vec::new(),
        tokens: Vec::new(),
    }
}

const NAMED: &str = "0x20c0000000000000000000000000000000000001";
const FEE: &str = "0x20c0000000000000000000000000000000000002";

/// Under `ROLE=indexer`, a committed bundle notes the tokens it names that
/// have no metadata, once; the missing-metadata job drains them.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_indexer_notes_tokens_it_has_no_metadata_for() {
    let (_scratch, url) = backend::scratch_schema().await;
    let db = open(&url, Role::Indexer).await;
    db::save_block_bundle(&db, &bundle(1, NAMED, FEE))
        .await
        .unwrap();
    db::save_block_bundles(&db, &[bundle(2, NAMED, FEE)])
        .await
        .unwrap();

    let mut noted = db::take_tokens_without_metadata(&db);
    noted.sort();
    let mut want = vec![checksum_address(NAMED), checksum_address(FEE)];
    want.sort();
    assert_eq!(noted, want);
    assert!(db::take_tokens_without_metadata(&db).is_empty(), "drained");

    db::save_token_metadata(
        &db,
        &TokenMeta {
            address: checksum_address(NAMED),
            name: String::new(),
            symbol: String::new(),
            decimals: 0,
            currency: String::new(),
            total_supply: "0".into(),
        },
    )
    .await
    .unwrap();
    db::save_block_bundle(&db, &bundle(3, NAMED, FEE))
        .await
        .unwrap();
    // A token with an empty label still has metadata.
    assert_eq!(
        db::take_tokens_without_metadata(&db),
        vec![checksum_address(FEE)]
    );
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn only_the_indexer_notes() {
    let (_scratch, url) = backend::scratch_schema().await;
    let db = open(&url, Role::All).await;
    db::save_block_bundle(&db, &bundle(1, NAMED, FEE))
        .await
        .unwrap();
    assert!(db::take_tokens_without_metadata(&db).is_empty());
}

/// Backfill's frontier: an outage must read as an error, never as an empty
/// table, or backfill would re-walk the chain from the head.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_frontier_read_fails_rather_than_reading_empty() {
    let (_scratch, url) = backend::scratch_schema().await;
    drop(open(&url, Role::Indexer).await);
    // A web replica pointed at a port nothing listens on.
    let web = open(
        "postgres://explorer:explorer@127.0.0.1:1/explorer",
        Role::Web,
    )
    .await;
    let (min, failed) = db::track_failures(db::get_min_block_number(&web)).await;
    assert_eq!(min, None);
    assert!(failed, "the degraded read is flagged for a 503");
    assert!(db::try_min_block_number(&web).await.is_err());
    assert!(
        db::tokens_missing_metadata(&web).await.is_err(),
        "the missing-metadata scan fails too, so the job scans again"
    );
}

/// The manifests' shape: DATABASE_URL names the server and nothing else, and
/// the user and password come from PGUSER and PGPASSWORD, as a Secret would
/// inject them, so the URL never holds the password.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn credentials_come_from_pguser_and_pgpassword() {
    let (_scratch, url) = backend::scratch_schema().await;
    let mut bare = url::Url::parse(&url).unwrap();
    let user = bare.username().to_string();
    let password = bare
        .password()
        .expect("PG_TEST_URL carries a password")
        .to_string();
    bare.set_username("").unwrap();
    bare.set_password(None).unwrap();
    let env = std::collections::HashMap::from([
        ("ROLE", "indexer".to_string()),
        ("DATABASE_URL", bare.to_string()),
        ("PGUSER", user),
        ("PGPASSWORD", password.clone()),
    ]);
    let cfg = db::DbConfig::from_env(|key| env.get(key).cloned()).unwrap();
    assert_eq!(cfg.password_source(), db::PasswordSource::Env);
    let db::DbTarget::Postgres(target) = &cfg.target else {
        panic!("not Postgres: {:?}", cfg.target);
    };
    assert!(!target.has_password(), "{target}");

    let db = db::open_with(
        &cfg,
        tokio::sync::watch::channel(Status::starting(Role::Indexer)).0,
    )
    .await
    .expect("connect with PGUSER and PGPASSWORD");
    db::save_block_bundle(
        &db,
        &bundle(
            7,
            "0x20c0000000000000000000000000000000000001",
            "0x20c0000000000000000000000000000000000001",
        ),
    )
    .await
    .unwrap();
    assert_eq!(db::get_latest_block(&db).await.map(|b| b.number), Some(7));
}
