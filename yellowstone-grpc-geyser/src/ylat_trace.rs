//! Per-message delivery stage stamps for `transaction_accounts`, compiled only with the
//! `ylat-trace` feature (used by the `ylat-harness` benchmark; never in production builds).
//!
//! Stages: plugin callback (`created_at`) -> geyser loop dequeued it -> subscriber loop
//! received the broadcast batch -> tonic encoded it for the HTTP/2 stream.

use {
    solana_signature::Signature,
    std::{
        collections::HashMap,
        io::Write,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    },
};

type Record = (Signature, u64, u64, u64, u64);

static STAGES: Mutex<Option<HashMap<Signature, [u64; 2]>>> = Mutex::new(None);
static RECORDS: Mutex<Vec<Record>> = Mutex::new(Vec::new());

#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn stamp_loop(signature: &Signature, now: u64) {
    let mut stages = STAGES.lock().unwrap();
    let map = stages.get_or_insert_with(HashMap::new);
    if map.len() > 500_000 {
        map.clear();
    }
    map.insert(*signature, [now, 0]);
}

pub fn stamp_client(signature: &Signature, now: u64) {
    if let Some(entry) = STAGES
        .lock()
        .unwrap()
        .as_mut()
        .and_then(|map| map.get_mut(signature))
    {
        if entry[1] == 0 {
            entry[1] = now;
        }
    }
}

pub fn stamp_encode(signature: &Signature, created_ns: u64) {
    let now = now_ns();
    let [loop_ns, client_ns] = STAGES
        .lock()
        .unwrap()
        .as_mut()
        .and_then(|map| map.remove(signature))
        .unwrap_or([0, 0]);
    RECORDS
        .lock()
        .unwrap()
        .push((*signature, created_ns, loop_ns, client_ns, now));
}

/// Writes all records as CSV; returns the number written.
pub fn dump(path: &str) -> std::io::Result<usize> {
    let records = std::mem::take(&mut *RECORDS.lock().unwrap());
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(out, "signature,created_ns,loop_ns,client_ns,encode_ns")?;
    for (signature, created, loop_ns, client_ns, encode_ns) in &records {
        writeln!(out, "{signature},{created},{loop_ns},{client_ns},{encode_ns}")?;
    }
    out.flush()?;
    Ok(records.len())
}
