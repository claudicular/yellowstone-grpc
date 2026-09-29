//! Standalone delivery-latency harness: loads the plugin in-process (no validator) and
//! replays a recorded mainnet `transaction_accounts` trace through the same Geyser callbacks
//! agave calls, with the recorded timing, from several producer threads (like agave's
//! fast-lane commit threads). Measure with `ylat-probe` against the harness port.
//!
//! Per transaction it calls `notify_transaction_accounts` and then `update_account_for_bank`
//! for each of its accounts (agave emits the grouped notification before storing the batch);
//! a slot thread emits the slot lifecycle (first shred, created bank, completed, block meta,
//! processed/confirmed/rooted) and producers emit an entry every few transactions.
//!
//! Trace format (from `ylat-probe --include-all-accounts --record`): one line per
//! transaction: `created_ns slot index signature pubkey:owner:data_len ...`.
//!
//! ```bash
//! ylat-harness --config harness-config.json --trace trace_all.txt --warmup-s 5
//! ```

use {
    clap::Parser,
    solana_hash::Hash,
    solana_message::{legacy::Message as LegacyMessage, MessageHeader, VersionedMessage},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status::TransactionStatusMeta,
    std::{
        collections::BTreeMap,
        io::{BufRead, BufReader},
        str::FromStr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, Instant},
    },
    yellowstone_grpc_geyser::{
        plugin::entry::Plugin,
        plugin_interface::geyser_plugin_interface::{
            GeyserPlugin, ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV4,
            ReplicaBlockInfoVersions, ReplicaDeshredTransactionInfo,
            ReplicaDeshredTransactionInfoVersions, ReplicaEntryInfoV2, ReplicaEntryInfoVersions,
            ReplicaTransactionAccountsInfo, ReplicaTransactionAccountsInfoVersions,
            ReplicaTransactionInfoV3, ReplicaTransactionInfoVersions, SlotStatus,
        },
    },
};

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Plugin config (use a port other than the validator's, e.g. 127.0.0.1:10077).
    #[clap(long)]
    config: String,
    #[clap(long)]
    trace: String,
    /// Producer threads (agave fast lane: 3 commit threads).
    #[clap(long, default_value_t = 3)]
    producers: usize,
    /// Seconds to wait after loading the plugin before replaying (connect the probe now).
    #[clap(long, default_value_t = 5)]
    warmup_s: u64,
    /// Replay speed multiplier.
    #[clap(long, default_value_t = 1.0)]
    speed: f64,
    /// Stop after this many seconds of trace (0 = whole trace).
    #[clap(long, default_value_t = 0)]
    seconds: u64,
    /// Emit an entry notification every N transactions per producer.
    #[clap(long, default_value_t = 8)]
    entry_every: usize,
    /// Also emit `notify_transaction_for_bank` for every transaction this long after its
    /// grouped accounts (agave's transaction-status thread), in microseconds; negative = off.
    #[clap(long, default_value_t = 300, allow_hyphen_values = true)]
    txstatus_after_us: i64,
    /// Also emit `notify_deshred_transaction` for every transaction this long before its
    /// grouped accounts, in microseconds; negative = off.
    #[clap(long, default_value_t = 1500, allow_hyphen_values = true)]
    deshred_before_us: i64,
    /// Where to write per-message stage stamps (in-process plugin only).
    #[clap(long, default_value = "/dev/shm/ylat_stages.csv")]
    stages_out: String,
    /// Load the plugin from this `.so` (like agave) instead of in-process.
    #[clap(long)]
    lib: Option<String>,
    /// Reload test: at `--reload-at-s` into the replay, unload the plugin and load this
    /// `.so` with `--reload-config` (is_reload = true), as `agave-validator plugin reload`.
    #[clap(long)]
    reload_lib: Option<String>,
    #[clap(long)]
    reload_config: Option<String>,
    #[clap(long, default_value_t = 10)]
    reload_at_s: u64,
}

/// The loaded plugin, reachable from every emitting thread. Like agave's plugin manager
/// during `plugin reload`, callbacks that arrive while the slot is being swapped are
/// dropped (agave stores a manager without the plugin while it unloads/loads).
struct PluginSlot {
    plugin: RwLock<Option<Box<dyn GeyserPlugin>>>,
    library: Mutex<Option<libloading::Library>>,
    dropped: AtomicU64,
}

impl PluginSlot {
    fn with(&self, f: impl FnOnce(&dyn GeyserPlugin)) {
        match self.plugin.try_read() {
            Ok(guard) => match guard.as_ref() {
                Some(plugin) => f(plugin.as_ref()),
                None => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// In-process plugin (`lib` = None) or a plugin `.so` loaded like agave does.
    fn load(&self, lib: Option<&str>, config: &str, is_reload: bool) -> anyhow::Result<()> {
        let mut plugin: Box<dyn GeyserPlugin> = match lib {
            None => Box::new(Plugin::default()),
            Some(path) => unsafe {
                #[allow(improper_ctypes_definitions)]
                type CreatePlugin = unsafe extern "C" fn() -> *mut dyn GeyserPlugin;
                let library = libloading::Library::new(path)?;
                let create: libloading::Symbol<CreatePlugin> = library.get(b"_create_plugin")?;
                let plugin = Box::from_raw(create());
                *self.library.lock().unwrap() = Some(library);
                plugin
            },
        };
        let t = Instant::now();
        plugin
            .on_load(config, is_reload)
            .map_err(|e| anyhow::anyhow!("on_load: {e}"))?;
        eprintln!(
            "ylat-harness: loaded {} ({}) in {:?}",
            plugin.name(),
            lib.unwrap_or("in-process"),
            t.elapsed()
        );
        *self.plugin.write().unwrap() = Some(plugin);
        Ok(())
    }

    /// Unload: on_unload, drop the plugin, then drop (dlclose) its library.
    fn unload(&self) {
        let plugin = self.plugin.write().unwrap().take();
        if let Some(mut plugin) = plugin {
            let t = Instant::now();
            plugin.on_unload();
            drop(plugin);
            eprintln!("ylat-harness: unloaded in {:?}", t.elapsed());
        }
        drop(self.library.lock().unwrap().take());
    }
}

fn thread_census() -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
        for entry in dir.flatten() {
            let name = std::fs::read_to_string(entry.path().join("comm")).unwrap_or_default();
            let name = name
                .trim()
                .trim_end_matches(|c: char| c.is_ascii_digit())
                .to_owned();
            *counts.entry(name).or_default() += 1;
        }
    }
    counts
        .iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join(" ")
}

const SYSVARS: [Pubkey; 4] = [
    solana_pubkey::pubkey!("SysvarC1ock11111111111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarS1otHashes111111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarS1otHistory11111111111111111111111111"),
    solana_pubkey::pubkey!("SysvarRecentB1ockHashes11111111111111111111"),
];
const SYSVAR_OWNER: Pubkey = solana_pubkey::pubkey!("Sysvar1111111111111111111111111111111111111");

struct Tx {
    rel_ns: u64,
    slot: u64,
    index: usize,
    signature: Signature,
    accounts: Vec<(Pubkey, Pubkey, usize)>,
}

fn load_trace(path: &str, max_ns: u64) -> anyhow::Result<Vec<Tx>> {
    let mut txs = Vec::new();
    let mut first = None;
    for line in BufReader::new(std::fs::File::open(path)?).lines() {
        let line = line?;
        let mut parts = line.split(' ');
        let created: u64 = parts.next().unwrap_or("0").parse()?;
        let slot: u64 = parts.next().unwrap_or("0").parse()?;
        let index: usize = parts.next().unwrap_or("0").parse()?;
        let signature = Signature::from_str(parts.next().unwrap_or(""))
            .map_err(|e| anyhow::anyhow!("bad signature: {e}"))?;
        let mut accounts = Vec::new();
        for account in parts {
            let mut f = account.split(':');
            let pubkey = Pubkey::from_str(f.next().unwrap_or(""))?;
            let owner = Pubkey::from_str(f.next().unwrap_or(""))?;
            let len: usize = f.next().unwrap_or("0").parse()?;
            accounts.push((pubkey, owner, len));
        }
        let first = *first.get_or_insert(created);
        let rel_ns = created.saturating_sub(first);
        if max_ns != 0 && rel_ns > max_ns {
            break;
        }
        txs.push(Tx {
            rel_ns,
            slot,
            index,
            signature,
            accounts,
        });
    }
    txs.sort_by_key(|tx| tx.rel_ns);
    Ok(txs)
}

fn wait_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        let left = deadline - now;
        if left > Duration::from_micros(200) {
            std::thread::sleep(left - Duration::from_micros(150));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn account<'a>(
    pubkey: &'a Pubkey,
    owner: &'a Pubkey,
    data: &'a [u8],
    wv: u64,
) -> ReplicaAccountInfoV3<'a> {
    ReplicaAccountInfoV3 {
        pubkey: pubkey.as_ref(),
        lamports: 2_039_280,
        owner: owner.as_ref(),
        executable: false,
        rent_epoch: u64::MAX,
        data,
        write_version: wv,
        txn: None,
    }
}

/// A minimal legacy transaction carrying the trace's account keys (and their owners as
/// program ids, so account filters on a program match like they would on mainnet).
fn versioned_tx(tx: &Tx) -> VersionedTransaction {
    let mut keys: Vec<Pubkey> = Vec::with_capacity(tx.accounts.len() * 2);
    for (pubkey, owner, _) in &tx.accounts {
        for key in [pubkey, owner] {
            if !keys.contains(key) {
                keys.push(*key);
            }
        }
    }
    VersionedTransaction {
        signatures: vec![tx.signature],
        message: VersionedMessage::Legacy(LegacyMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: keys,
            recent_blockhash: Hash::default(),
            instructions: vec![],
        }),
    }
}

fn status_meta(n_keys: usize) -> TransactionStatusMeta {
    TransactionStatusMeta {
        fee: 5000,
        pre_balances: vec![1_000_000; n_keys],
        post_balances: vec![999_000; n_keys],
        log_messages: Some(
            (0..12)
                .map(|i| {
                    format!("Program log: instruction {i} consumed 12345 of 200000 compute units")
                })
                .collect(),
        ),
        inner_instructions: Some(vec![]),
        pre_token_balances: Some(vec![]),
        post_token_balances: Some(vec![]),
        compute_units_consumed: Some(123_456),
        ..TransactionStatusMeta::default()
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let max_ns = args.seconds * 1_000_000_000;
    let txs = Arc::new(load_trace(&args.trace, max_ns)?);
    let max_len = txs
        .iter()
        .flat_map(|tx| tx.accounts.iter().map(|a| a.2))
        .max()
        .unwrap_or(0);
    let data: Arc<Vec<u8>> = Arc::new((0..max_len).map(|i| (i % 251) as u8).collect());
    eprintln!(
        "ylat-harness: {} transactions over {:.1} s, max account data {} bytes",
        txs.len(),
        txs.last().map(|tx| tx.rel_ns).unwrap_or(0) as f64 / 1e9,
        max_len
    );

    eprintln!("ylat-harness: threads before load: {}", thread_census());
    let plugin = Arc::new(PluginSlot {
        plugin: RwLock::new(None),
        library: Mutex::new(None),
        dropped: AtomicU64::new(0),
    });
    plugin.load(args.lib.as_deref(), &args.config, false)?;
    eprintln!("ylat-harness: threads after load: {}", thread_census());
    eprintln!("ylat-harness: plugin loaded, replay in {} s", args.warmup_s);
    std::thread::sleep(Duration::from_secs(args.warmup_s));

    // Slot lifecycle schedule: open each slot 1 ms before its first transaction and close
    // it 2 ms after its last.
    let mut slots: BTreeMap<u64, (u64, u64, u64)> = BTreeMap::new(); // slot -> (first, last, count)
    for tx in txs.iter() {
        let e = slots.entry(tx.slot).or_insert((tx.rel_ns, tx.rel_ns, 0));
        e.0 = e.0.min(tx.rel_ns);
        e.1 = e.1.max(tx.rel_ns);
        e.2 += 1;
    }
    let mut events: Vec<(u64, u64, u8)> = Vec::new(); // (time, slot, 0=open 1=close)
    for (slot, (first, last, _)) in &slots {
        events.push((first.saturating_sub(1_000_000), *slot, 0));
        events.push((last + 2_000_000, *slot, 1));
    }
    events.sort();
    let slots = Arc::new(slots);

    let write_version = Arc::new(AtomicU64::new(1));
    let speed = args.speed;
    let start = Instant::now() + Duration::from_millis(50);
    let at = move |rel_ns: u64| start + Duration::from_nanos((rel_ns as f64 / speed) as u64);

    let mut handles = Vec::new();
    {
        let plugin = Arc::clone(&plugin);
        let slots = Arc::clone(&slots);
        let write_version = Arc::clone(&write_version);
        let txstatus = args.txstatus_after_us >= 0;
        handles.push(
            std::thread::Builder::new()
                .name("ylatSlots".into())
                .spawn(move || {
                    let mut closed: Vec<u64> = Vec::new();
                    let rewards = solana_transaction_status::RewardsAndNumPartitions {
                        rewards: Vec::new(),
                        num_partitions: None,
                    };
                    for (time, slot, kind) in events {
                        wait_until(at(time));
                        let parent = Some(slot.saturating_sub(1));
                        // A bank seals (Block messages, confirmed/finalized replays) only after agave's
                        // sysvar writes: Clock and SlotHashes at bank creation, the rest at freeze.
                        let sysvars = |keys: &[Pubkey]| {
                            let data = slot.to_le_bytes();
                            for key in keys {
                                let info = account(
                                    key,
                                    &SYSVAR_OWNER,
                                    &data,
                                    write_version.fetch_add(1, Ordering::Relaxed),
                                );
                                plugin.with(|p| {
                                    let _ = p.update_account_for_bank(
                                        ReplicaAccountInfoVersions::V0_0_3(&info),
                                        slot,
                                        slot,
                                    );
                                });
                            }
                        };
                        if kind == 0 {
                            sysvars(&SYSVARS[..2]);
                            plugin.with(|p| {
                                let _ = p.update_slot_status(
                                    slot,
                                    parent,
                                    &SlotStatus::FirstShredReceived,
                                );
                            });
                            plugin.with(|p| {
                                let _ = p.update_bank_status(
                                    slot,
                                    parent,
                                    &SlotStatus::CreatedBank,
                                    slot,
                                );
                            });
                        } else {
                            let count = if txstatus {
                                slots.get(&slot).map(|s| s.2).unwrap_or(0)
                            } else {
                                0
                            };
                            sysvars(&SYSVARS[2..]);
                            plugin.with(|p| {
                                let _ = p.update_slot_status(slot, parent, &SlotStatus::Completed);
                            });
                            let hash = format!("{slot:044}");
                            let parent_hash = format!("{:044}", slot.saturating_sub(1));
                            let info = ReplicaBlockInfoV4 {
                                parent_slot: slot.saturating_sub(1),
                                parent_blockhash: &parent_hash,
                                slot,
                                blockhash: &hash,
                                rewards: &rewards,
                                block_time: Some(0),
                                block_height: Some(slot),
                                executed_transaction_count: count,
                                entry_count: 0,
                            };
                            plugin.with(|p| {
                                let _ = p.notify_block_metadata_for_bank(
                                    ReplicaBlockInfoVersions::V0_0_4(&info),
                                    slot,
                                );
                                let _ = p.update_bank_status(
                                    slot,
                                    parent,
                                    &SlotStatus::Processed,
                                    slot,
                                );
                            });
                            closed.push(slot);
                            if closed.len() > 2 {
                                let c = closed[closed.len() - 3];
                                plugin.with(|p| {
                                    let _ = p.update_bank_status(
                                        c,
                                        Some(c.saturating_sub(1)),
                                        &SlotStatus::Confirmed,
                                        c,
                                    );
                                });
                            }
                            if closed.len() > 32 {
                                let r = closed[closed.len() - 33];
                                plugin.with(|p| {
                                    let _ = p.update_bank_status(
                                        r,
                                        Some(r.saturating_sub(1)),
                                        &SlotStatus::Rooted,
                                        r,
                                    );
                                });
                            }
                        }
                    }
                })?,
        );
    }

    // agave's transaction-status thread (after commit) and the deshred path (before
    // execution) emit from their own threads.
    for (name, offset_us) in [
        ("ylatTxStatus", args.txstatus_after_us),
        ("ylatDeshred", -args.deshred_before_us),
    ] {
        if (name == "ylatTxStatus" && args.txstatus_after_us < 0)
            || (name == "ylatDeshred" && args.deshred_before_us < 0)
        {
            continue;
        }
        let plugin = Arc::clone(&plugin);
        let txs = Arc::clone(&txs);
        let deshred = name == "ylatDeshred";
        handles.push(
            std::thread::Builder::new()
                .name(name.into())
                .spawn(move || {
                    let hash = Hash::default();
                    for tx in txs.iter() {
                        let t = (tx.rel_ns as i64 + offset_us * 1000).max(0) as u64;
                        wait_until(at(t));
                        let vtx = versioned_tx(tx);
                        if deshred {
                            let info = ReplicaDeshredTransactionInfo {
                                signature: &tx.signature,
                                is_vote: false,
                                transaction: &vtx,
                                loaded_addresses: None,
                            };
                            plugin.with(|p| {
                                let _ = p.notify_deshred_transaction(
                                    ReplicaDeshredTransactionInfoVersions::V0_0_1(&info),
                                    tx.slot,
                                );
                            });
                        } else {
                            let meta = status_meta(vtx.message.static_account_keys().len());
                            let info = ReplicaTransactionInfoV3 {
                                signature: &tx.signature,
                                message_hash: &hash,
                                is_vote: false,
                                transaction: &vtx,
                                transaction_status_meta: &meta,
                                index: tx.index,
                            };
                            plugin.with(|p| {
                                let _ = p.notify_transaction_for_bank(
                                    ReplicaTransactionInfoVersions::V0_0_3(&info),
                                    tx.slot,
                                    tx.slot,
                                );
                            });
                        }
                    }
                })?,
        );
    }

    let callback_ns: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    for p in 0..args.producers {
        let callback_ns = Arc::clone(&callback_ns);
        let plugin = Arc::clone(&plugin);
        let txs = Arc::clone(&txs);
        let data = Arc::clone(&data);
        let write_version = Arc::clone(&write_version);
        let producers = args.producers;
        let entry_every = args.entry_every.max(1);
        handles.push(
            std::thread::Builder::new()
                .name(format!("ylatProd{p}"))
                .spawn(move || {
                    let mut n = 0usize;
                    let hash = [7u8; 32];
                    let mut cb_ns: Vec<u64> = Vec::with_capacity(txs.len() / producers + 1);
                    for tx in txs.iter() {
                        // Spread transactions over producers like agave's parallel commit threads.
                        if (tx.signature.as_ref()[0] as usize) % producers != p {
                            continue;
                        }
                        wait_until(at(tx.rel_ns));
                        let wv0 = write_version
                            .fetch_add(tx.accounts.len() as u64 * 2, Ordering::Relaxed);
                        let infos: Vec<ReplicaAccountInfoV3<'_>> = tx
                            .accounts
                            .iter()
                            .enumerate()
                            .map(|(i, (pubkey, owner, len))| {
                                account(pubkey, owner, &data[..*len], wv0 + i as u64)
                            })
                            .collect();
                        let grouped = ReplicaTransactionAccountsInfo {
                            signature: &tx.signature,
                            slot: tx.slot,
                            index: tx.index,
                            accounts: &infos,
                        };
                        let cb_start = Instant::now();
                        plugin.with(|p| {
                            let _ = p.notify_transaction_accounts(
                                ReplicaTransactionAccountsInfoVersions::V0_0_1(&grouped),
                                tx.slot,
                            );
                        });
                        for (i, (pubkey, owner, len)) in tx.accounts.iter().enumerate() {
                            let info = account(
                                pubkey,
                                owner,
                                &data[..*len],
                                wv0 + (tx.accounts.len() + i) as u64,
                            );
                            plugin.with(|p| {
                                let _ = p.update_account_for_bank(
                                    ReplicaAccountInfoVersions::V0_0_3(&info),
                                    tx.slot,
                                    tx.slot,
                                );
                            });
                        }
                        cb_ns.push(cb_start.elapsed().as_nanos() as u64);
                        n += 1;
                        if n.is_multiple_of(entry_every) {
                            let entry = ReplicaEntryInfoV2 {
                                slot: tx.slot,
                                index: n,
                                num_hashes: 1,
                                hash: &hash,
                                executed_transaction_count: entry_every as u64,
                                starting_transaction_index: tx.index,
                            };
                            plugin.with(|p| {
                                let _ = p.notify_entry_for_bank(
                                    ReplicaEntryInfoVersions::V0_0_2(&entry),
                                    tx.slot,
                                );
                            });
                        }
                    }
                    callback_ns.lock().unwrap().extend(cb_ns);
                })?,
        );
    }
    if let Some(reload_lib) = args.reload_lib.clone() {
        let reload_config = args
            .reload_config
            .clone()
            .unwrap_or_else(|| args.config.clone());
        wait_until(start + Duration::from_secs(args.reload_at_s));
        eprintln!("ylat-harness: reload: threads before: {}", thread_census());
        let t = Instant::now();
        plugin.unload();
        eprintln!(
            "ylat-harness: reload: threads after unload: {}",
            thread_census()
        );
        plugin.load(Some(&reload_lib), &reload_config, true)?;
        eprintln!(
            "ylat-harness: reload done in {:?}; threads after: {}",
            t.elapsed(),
            thread_census()
        );
    }
    for handle in handles {
        let _ = handle.join();
    }
    eprintln!(
        "ylat-harness: replay done in {:.1} s ({} callbacks dropped while unloaded)",
        start.elapsed().as_secs_f64(),
        plugin.dropped.load(Ordering::Relaxed)
    );
    {
        let mut v = std::mem::take(&mut *callback_ns.lock().unwrap());
        v.sort_unstable();
        let p = |q: f64| {
            v.get(((v.len() as f64 * q) as usize).min(v.len().saturating_sub(1)))
                .copied()
                .unwrap_or(0) as f64
                / 1000.0
        };
        eprintln!(
            "ylat-harness: per-tx callback time (tx_accounts + its account updates) us: p50 {:.1} p90 {:.1} p99 {:.1} p99.9 {:.1}",
            p(0.5), p(0.9), p(0.99), p(0.999)
        );
    }
    std::thread::sleep(Duration::from_secs(1));
    if args.lib.is_none() && args.reload_lib.is_none() {
        let n = yellowstone_grpc_geyser::ylat_trace::dump(&args.stages_out)?;
        eprintln!(
            "ylat-harness: wrote {n} stage records to {}",
            args.stages_out
        );
    }
    plugin.unload();
    eprintln!(
        "ylat-harness: threads after final unload: {}",
        thread_census()
    );
    Ok(())
}
