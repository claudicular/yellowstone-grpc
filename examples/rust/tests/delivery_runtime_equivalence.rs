//! The dedicated delivery runtime (`grpc_runtime`) must change timing only: for an identical
//! sequence of Geyser callbacks, a subscriber must receive exactly the same updates, in the
//! same order, as with the default single-runtime setup.
//!
//! Each configuration loads the plugin in-process on its own port, opens one processed and
//! one confirmed subscription covering accounts, transaction_accounts, slots, entries,
//! blocks_meta and blocks, replays the same deterministic callback sequence (several slots,
//! lifecycle included), and records every update with `created_at` stripped.

use {
    futures::{sink::SinkExt, stream::StreamExt},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{collections::HashMap, io::Write, time::Duration},
    yellowstone_grpc_geyser::{
        plugin::entry::Plugin,
        plugin_interface::geyser_plugin_interface::{
            GeyserPlugin, ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV4,
            ReplicaBlockInfoVersions, ReplicaEntryInfoV2, ReplicaEntryInfoVersions,
            ReplicaTransactionAccountsInfo, ReplicaTransactionAccountsInfoVersions, SlotStatus,
        },
    },
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, subscribe_update::UpdateOneof, CommitmentLevel,
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactionAccounts, SubscribeUpdate,
    },
};

const TOKEN: Pubkey = solana_pubkey::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const SLOTS: u64 = 6;
const TXS_PER_SLOT: usize = 150;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_config(port: u16, grpc_runtime: Option<&str>) -> tempfile_path::TempPath {
    let runtime = grpc_runtime
        .map(|r| format!(r#""grpc_runtime": {r},"#))
        .unwrap_or_default();
    let json = format!(
        r#"{{
  "libpath": "unused",
  "log": {{ "level": "error" }},
  {runtime}
  "grpc": {{
    "address": "127.0.0.1:{port}",
    "snapshot_plugin_channel_capacity": null,
    "channel_capacity": "100_000",
    "unary_concurrency_limit": 10
  }}
}}"#
    );
    let path = tempfile_path::TempPath::new(&format!("ylat-equiv-{port}.json"));
    std::fs::File::create(path.path())
        .unwrap()
        .write_all(json.as_bytes())
        .unwrap();
    path
}

/// Minimal temp file helper (no extra dev-dependency).
mod tempfile_path {
    pub struct TempPath(std::path::PathBuf);
    impl TempPath {
        pub fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(name))
        }
        pub fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

fn request(commitment: CommitmentLevel) -> SubscribeRequest {
    let key = |s: &str| s.to_owned();
    SubscribeRequest {
        accounts: HashMap::from([(
            key("acc"),
            SubscribeRequestFilterAccounts {
                owner: vec![TOKEN.to_string()],
                ..Default::default()
            },
        )]),
        transaction_accounts: HashMap::from([(
            key("txacc"),
            SubscribeRequestFilterTransactionAccounts {
                owner: vec![TOKEN.to_string()],
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(key("slots"), SubscribeRequestFilterSlots::default())]),
        entry: HashMap::from([(key("entry"), SubscribeRequestFilterEntry::default())]),
        blocks_meta: HashMap::from([(key("meta"), SubscribeRequestFilterBlocksMeta::default())]),
        blocks: HashMap::from([(
            key("blocks"),
            SubscribeRequestFilterBlocks {
                include_accounts: Some(true),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(commitment as i32),
        ..Default::default()
    }
}

/// A bank seals (blocks, confirmed/finalized replays) only after these sysvar writes: agave
/// writes Clock and SlotHashes as the bank is created and the other two as it freezes.
const SYSVARS: [Pubkey; 4] = [
    solana_pubkey::pubkey!("SysvarC1ock11111111111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarS1otHashes111111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarS1otHistory11111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarRecentB1ockHashes11111111111111111111"),
];
const SYSVAR_OWNER: Pubkey = solana_pubkey::pubkey!("Sysvar1111111111111111111111111111111111111");

fn emit_sysvars(plugin: &Plugin, slot: u64, keys: &[Pubkey], write_version: &mut u64) {
    let data = slot.to_le_bytes();
    for key in keys {
        let info = ReplicaAccountInfoV3 {
            pubkey: key.as_ref(),
            lamports: 1,
            owner: SYSVAR_OWNER.as_ref(),
            executable: false,
            rent_epoch: u64::MAX,
            data: &data,
            write_version: *write_version,
            txn: None,
        };
        *write_version += 1;
        plugin
            .update_account_for_bank(ReplicaAccountInfoVersions::V0_0_3(&info), slot, slot)
            .unwrap();
    }
}

/// Deterministic callback sequence (single thread, so the plugin queue order is fixed).
fn replay(plugin: &Plugin) {
    let rewards = solana_transaction_status::RewardsAndNumPartitions {
        rewards: vec![],
        num_partitions: None,
    };
    let data_token = vec![3u8; 165];
    let data_other = vec![9u8; 700];
    let system = Pubkey::default();
    let other_owner = Pubkey::new_from_array([5; 32]);
    let mut write_version = 1u64;
    let hash = [1u8; 32];
    let base = 1_000u64;
    for s in 0..SLOTS {
        let slot = base + s;
        let parent = Some(slot - 1);
        plugin
            .update_slot_status(slot, parent, &SlotStatus::FirstShredReceived)
            .unwrap();
        emit_sysvars(plugin, slot, &SYSVARS[..2], &mut write_version);
        plugin
            .update_bank_status(slot, parent, &SlotStatus::CreatedBank, slot)
            .unwrap();
        for i in 0..TXS_PER_SLOT {
            let mut sig = [0u8; 64];
            sig[..8].copy_from_slice(&slot.to_le_bytes());
            sig[8..16].copy_from_slice(&(i as u64).to_le_bytes());
            let signature = Signature::from(sig);
            // Shared keys make the same account change repeatedly across transactions.
            let keys: Vec<(Pubkey, Pubkey, &[u8])> = (0..(1 + i % 4))
                .map(|k| {
                    let mut pk = [0u8; 32];
                    pk[0] = ((i + k) % 23) as u8;
                    pk[1] = k as u8;
                    let (owner, data): (Pubkey, &[u8]) = match (i + k) % 3 {
                        0 => (TOKEN, &data_token),
                        1 => (other_owner, &data_other),
                        _ => (system, &[]),
                    };
                    (Pubkey::new_from_array(pk), owner, data)
                })
                .collect();
            let infos: Vec<ReplicaAccountInfoV3> = keys
                .iter()
                .enumerate()
                .map(|(k, (pubkey, owner, data))| ReplicaAccountInfoV3 {
                    pubkey: pubkey.as_ref(),
                    lamports: 1_000 + (i + k) as u64,
                    owner: owner.as_ref(),
                    executable: false,
                    rent_epoch: u64::MAX,
                    data,
                    write_version: write_version + k as u64,
                    txn: None,
                })
                .collect();
            plugin
                .notify_transaction_accounts(
                    ReplicaTransactionAccountsInfoVersions::V0_0_1(&ReplicaTransactionAccountsInfo {
                        signature: &signature,
                        slot,
                        index: i,
                        accounts: &infos,
                    }),
                    slot,
                )
                .unwrap();
            for info in &infos {
                plugin
                    .update_account_for_bank(ReplicaAccountInfoVersions::V0_0_3(info), slot, slot)
                    .unwrap();
            }
            write_version += infos.len() as u64;
            if i % 10 == 9 {
                plugin
                    .notify_entry_for_bank(
                        ReplicaEntryInfoVersions::V0_0_2(&ReplicaEntryInfoV2 {
                            slot,
                            index: i / 10,
                            num_hashes: 1,
                            hash: &hash,
                            executed_transaction_count: 10,
                            starting_transaction_index: i - 9,
                        }),
                        slot,
                    )
                    .unwrap();
            }
        }
        emit_sysvars(plugin, slot, &SYSVARS[2..], &mut write_version);
        plugin
            .update_slot_status(slot, parent, &SlotStatus::Completed)
            .unwrap();
        let blockhash = format!("{slot:044}");
        let parent_blockhash = format!("{:044}", slot - 1);
        plugin
            .notify_block_metadata_for_bank(
                ReplicaBlockInfoVersions::V0_0_4(&ReplicaBlockInfoV4 {
                    parent_slot: slot - 1,
                    parent_blockhash: &parent_blockhash,
                    slot,
                    blockhash: &blockhash,
                    rewards: &rewards,
                    block_time: Some(1),
                    block_height: Some(slot),
                    executed_transaction_count: 0,
                    entry_count: (TXS_PER_SLOT / 10) as u64,
                }),
                slot,
            )
            .unwrap();
        // Commitment statuses go straight to block reconstruction (not through the geyser
        // loop), so where they land relative to still-queued data of the current slot is
        // timing-dependent in every configuration; let the geyser loop drain first.
        std::thread::sleep(Duration::from_millis(150));
        plugin
            .update_bank_status(slot, parent, &SlotStatus::Processed, slot)
            .unwrap();
        if s >= 1 {
            let c = slot - 1;
            plugin
                .update_bank_status(c, Some(c - 1), &SlotStatus::Confirmed, c)
                .unwrap();
        }
        // Block reconstruction runs concurrently with the geyser loop and publishes its
        // Processed/Confirmed outputs into the same streams; let it settle so the expected
        // interleaving is deterministic (it is timing-dependent in every configuration).
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn normalize(mut update: SubscribeUpdate) -> Option<SubscribeUpdate> {
    if matches!(
        update.update_oneof,
        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_))
    ) {
        return None;
    }
    update.created_at = None;
    Some(update)
}

async fn collect(
    port: u16,
    commitment: CommitmentLevel,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Vec<SubscribeUpdate> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = GeyserClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
    let (mut tx, rx) = futures::channel::mpsc::unbounded();
    tx.send(request(commitment)).await.unwrap();
    let mut stream = client.subscribe(rx).await.unwrap().into_inner();
    // The filter is applied asynchronously; give the client loop time to install it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = ready.send(());
    let mut out = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(update))) => out.extend(normalize(update)),
            _ => break,
        }
    }
    let _keep = tx;
    out
}

fn run(grpc_runtime: Option<&str>) -> (Vec<SubscribeUpdate>, Vec<SubscribeUpdate>) {
    let port = free_port();
    let config = write_config(port, grpc_runtime);
    let mut plugin = Plugin::default();
    plugin
        .on_load(config.path().to_str().unwrap(), false)
        .expect("on_load");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (ready_p, wait_p) = tokio::sync::oneshot::channel();
    let (ready_c, wait_c) = tokio::sync::oneshot::channel();
    let processed = rt.spawn(collect(port, CommitmentLevel::Processed, ready_p));
    let confirmed = rt.spawn(collect(port, CommitmentLevel::Confirmed, ready_c));
    rt.block_on(async {
        wait_p.await.unwrap();
        wait_c.await.unwrap();
    });
    replay(&plugin);
    let processed = rt.block_on(processed).unwrap();
    let confirmed = rt.block_on(confirmed).unwrap();
    plugin.on_unload();
    (processed, confirmed)
}

#[test]
fn delivery_runtime_changes_timing_only() {
    let (base_p, base_c) = run(None);
    let kinds = |v: &[SubscribeUpdate]| {
        let mut m = std::collections::BTreeMap::<String, usize>::new();
        for u in v {
            let k = format!("{:?}", u.update_oneof.as_ref().map(std::mem::discriminant));
            let name = match &u.update_oneof {
                Some(UpdateOneof::Account(_)) => "account",
                Some(UpdateOneof::TransactionAccounts(_)) => "tx_accounts",
                Some(UpdateOneof::Slot(_)) => "slot",
                Some(UpdateOneof::Entry(_)) => "entry",
                Some(UpdateOneof::BlockMeta(_)) => "block_meta",
                Some(UpdateOneof::Block(_)) => "block",
                _ => k.as_str(),
            }
            .to_owned();
            *m.entry(name).or_default() += 1;
        }
        m
    };
    eprintln!("baseline processed: {:?}", kinds(&base_p));
    eprintln!("baseline confirmed: {:?}", kinds(&base_c));
    assert!(
        base_p
            .iter()
            .any(|u| matches!(u.update_oneof, Some(UpdateOneof::TransactionAccounts(_)))),
        "baseline received no transaction_accounts"
    );
    assert!(
        base_c
            .iter()
            .any(|u| matches!(u.update_oneof, Some(UpdateOneof::Block(_)))),
        "baseline confirmed stream received no blocks"
    );
    for runtime in [
        r#"{ "worker_threads": 1, "busy_poll": true }"#,
        r#"{ "worker_threads": 1 }"#,
        r#"{ "worker_threads": 2, "busy_poll": true }"#,
    ] {
        let (p, c) = run(Some(runtime));
        assert_eq!(p.len(), base_p.len(), "processed count differs for {runtime}");
        assert_eq!(c.len(), base_c.len(), "confirmed count differs for {runtime}");
        for (i, (a, b)) in p.iter().zip(&base_p).enumerate() {
            assert_eq!(a, b, "processed update #{i} differs for {runtime}");
        }
        for (i, (a, b)) in c.iter().zip(&base_c).enumerate() {
            assert_eq!(a, b, "confirmed update #{i} differs for {runtime}");
        }
    }
}
