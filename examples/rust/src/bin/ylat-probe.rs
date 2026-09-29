//! Minimal latency probe for `transaction_accounts` (field 100) delivery.
//!
//! Subscribes like geyserbench's `yellowstone_tx_accounts` provider (owner filter, no
//! include_all_accounts) and records, per message, the server `created_at` and the local
//! CLOCK_REALTIME receive time. It can run on a single pinned thread with a spinning
//! current-thread runtime, so the client-side share of the delivery latency is as small as
//! it can be; a fixed local port lets `tcpdump` isolate this connection and stamp the
//! moment the server wrote each message to the socket.
//!
//! ```bash
//! ylat-probe --endpoint http://127.0.0.1:10000 --seconds 60 --cpu 47 --spin \
//!     --local-port 41234 --out /dev/shm/probe.csv
//! ```

use {
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    hyper_util::rt::TokioIo,
    std::{
        collections::HashMap,
        io::Write,
        net::SocketAddr,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::net::TcpSocket,
    tonic::transport::{Endpoint, Uri},
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, subscribe_update::UpdateOneof, CommitmentLevel,
        SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterDeshredTransactions,
        SubscribeRequestFilterTransactionAccounts, SubscribeRequestPing,
    },
};

#[derive(Debug, Clone, Parser)]
struct Args {
    #[clap(long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,
    #[clap(long, default_value = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")]
    owner: String,
    #[clap(long, default_value_t = 30)]
    seconds: u64,
    /// Pin the (single) runtime thread to this CPU.
    #[clap(long)]
    cpu: Option<usize>,
    /// Keep the runtime hot with a yielding task (the I/O driver is polled every 61 ticks
    /// instead of after a futex/epoll sleep).
    #[clap(long)]
    spin: bool,
    /// Bind the client socket to this local port (0 = ephemeral).
    #[clap(long, default_value_t = 0)]
    local_port: u16,
    /// Wait this long between opening the stream and sending the filter (lets a packet
    /// capture start before the first DATA frame).
    #[clap(long, default_value_t = 0)]
    delay_subscribe_ms: u64,
    #[clap(long, default_value = "/dev/shm/ylat_probe.csv")]
    out: String,
    /// Request every account of a matching transaction (not only the owner-matched ones).
    #[clap(long)]
    include_all_accounts: bool,
    /// Also write a replayable load trace (one line per message: created_ns slot index
    /// signature then pubkey:owner:data_len per account) for ylat-harness.
    #[clap(long)]
    record: Option<String>,
    /// Also hold a deshred subscription (non-vote, account_include = owner) on a second
    /// connection, like geyserbench does, so the server sees the same client mix.
    #[clap(long)]
    deshred: bool,
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    #[cfg(target_os = "linux")]
    if let Some(cpu) = args.cpu {
        yellowstone_grpc_geyser::util::cpu_core_affinity::set_thread_affinity(&[cpu])
            .map_err(anyhow::Error::msg)?;
    }
    rt.block_on(run(args))
}

async fn run(args: Args) -> anyhow::Result<()> {
    if args.spin {
        tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
    }

    let target: SocketAddr = args
        .endpoint
        .trim_start_matches("http://")
        .parse()
        .map_err(|e| anyhow::anyhow!("endpoint must be http://ip:port: {e}"))?;
    let local_port = args.local_port;
    let channel = Endpoint::from_shared(args.endpoint.clone())?
        .tcp_nodelay(true)
        .connect_with_connector(tower::service_fn(move |_: Uri| async move {
            let socket = if target.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            socket.set_reuseaddr(true)?;
            socket.set_nodelay(true)?;
            if local_port != 0 {
                let local: SocketAddr = if target.is_ipv4() {
                    ([127, 0, 0, 1], local_port).into()
                } else {
                    (std::net::Ipv6Addr::LOCALHOST, local_port).into()
                };
                socket.bind(local)?;
            }
            let stream = socket.connect(target).await?;
            Ok::<_, std::io::Error>(TokioIo::new(stream))
        }))
        .await?;
    let mut client = GeyserClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);

    if args.deshred {
        let channel = Endpoint::from_shared(args.endpoint.clone())?.connect().await?;
        let mut deshred_client =
            GeyserClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
        let (mut dtx, drx) = futures::channel::mpsc::unbounded::<SubscribeDeshredRequest>();
        let mut filters = HashMap::new();
        filters.insert(
            "deshred".to_owned(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: vec![args.owner.clone()],
                ..Default::default()
            },
        );
        dtx.send(SubscribeDeshredRequest {
            deshred_transactions: filters,
            ..Default::default()
        })
        .await?;
        let mut dstream = deshred_client.subscribe_deshred(drx).await?.into_inner();
        tokio::spawn(async move {
            let mut n = 0u64;
            while let Some(Ok(msg)) = dstream.next().await {
                if matches!(
                    msg.update_oneof,
                    Some(yellowstone_grpc_proto::prelude::subscribe_update_deshred::UpdateOneof::Ping(_))
                ) {
                    let _ = dtx
                        .send(SubscribeDeshredRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await;
                }
                n += 1;
            }
            eprintln!("ylat-probe: deshred stream ended after {n} messages");
        });
    }

    let (mut tx, rx) = futures::channel::mpsc::unbounded::<SubscribeRequest>();
    let mut stream = client.subscribe(rx).await?.into_inner();
    eprintln!("ylat-probe: stream open, pid {}", std::process::id());
    if args.delay_subscribe_ms > 0 {
        tokio::time::sleep(Duration::from_millis(args.delay_subscribe_ms)).await;
    }
    let mut filters = HashMap::new();
    filters.insert(
        "account".to_owned(),
        SubscribeRequestFilterTransactionAccounts {
            owner: vec![args.owner.clone()],
            account: vec![],
            include_all_accounts: args.include_all_accounts.then_some(true),
            readonly_mints_only: Some(false),
        },
    );
    tx.send(SubscribeRequest {
        transaction_accounts: filters,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
    .await?;
    eprintln!("ylat-probe: subscribed");

    let mut rows: Vec<(u64, u64, u64, u32, Vec<u8>)> = Vec::with_capacity(1 << 20);
    let mut record = match &args.record {
        Some(path) => Some(std::io::BufWriter::new(std::fs::File::create(path)?)),
        None => None,
    };
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    loop {
        let msg = tokio::select! {
            m = stream.next() => m,
            _ = tokio::time::sleep_until(deadline.into()) => break,
        };
        let recv_ns = now_ns();
        let msg = match msg {
            Some(Ok(msg)) => msg,
            Some(Err(status)) => {
                eprintln!("ylat-probe: stream ended: {status}");
                break;
            }
            None => break,
        };
        match msg.update_oneof {
            Some(UpdateOneof::TransactionAccounts(u)) => {
                let created = msg
                    .created_at
                    .map(|t| t.seconds as u64 * 1_000_000_000 + t.nanos as u64)
                    .unwrap_or(0);
                if let Some(record) = record.as_mut() {
                    write!(
                        record,
                        "{created} {} {} {}",
                        u.slot,
                        u.index,
                        bs58::encode(&u.signature).into_string()
                    )?;
                    for account in &u.accounts {
                        write!(
                            record,
                            " {}:{}:{}",
                            bs58::encode(&account.pubkey).into_string(),
                            bs58::encode(&account.owner).into_string(),
                            account.data.len()
                        )?;
                    }
                    writeln!(record)?;
                }
                rows.push((recv_ns, created, u.slot, u.accounts.len() as u32, u.signature));
            }
            Some(UpdateOneof::Ping(_)) => {
                let _ = tx
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await;
            }
            _ => {}
        }
    }

    if let Some(mut record) = record {
        record.flush()?;
    }
    let mut out = std::io::BufWriter::new(std::fs::File::create(&args.out)?);
    writeln!(out, "signature,slot,accounts,server_created_unix_ns,recv_unix_ns")?;
    let mut lat: Vec<f64> = Vec::with_capacity(rows.len());
    for (recv, created, slot, n, sig) in &rows {
        writeln!(
            out,
            "{},{slot},{n},{created},{recv}",
            bs58::encode(sig).into_string()
        )?;
        lat.push((*recv as f64 - *created as f64) / 1e6);
    }
    out.flush()?;
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| lat.get(((lat.len() as f64 * q) as usize).min(lat.len().saturating_sub(1))).copied().unwrap_or(f64::NAN);
    eprintln!(
        "ylat-probe: {} msgs created->recv ms p50 {:.3} p90 {:.3} p99 {:.3} p99.9 {:.3}",
        lat.len(),
        p(0.5),
        p(0.9),
        p(0.99),
        p(0.999)
    );
    Ok(())
}
