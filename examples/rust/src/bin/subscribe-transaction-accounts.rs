//! Example: Subscribe to Transaction-Grouped Account Updates
//!
//! This example demonstrates how to use the `transaction_accounts` subscription
//! to receive all account updates for a transaction in a single message.
//!
//! Use case: Arbitrage bot monitoring DEX swaps
//! - Subscribe to transactions that touch Raydium AMM accounts
//! - Receive all writable accounts + Token mints in one grouped update
//! - No need to correlate individual account updates with transactions
//!
//! Run with:
//! ```bash
//! cargo run --bin subscribe-transaction-accounts -- \
//!     --endpoint http://127.0.0.1:10000 \
//!     --program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
//! ```

use {
    clap::Parser,
    futures::stream::StreamExt,
    log::{info, warn},
    std::env,
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterTransactionAccounts, SubscribeUpdateTransactionAccounts,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(
    author,
    version,
    about = "Subscribe to transaction-grouped account updates"
)]
struct Args {
    /// Yellowstone gRPC endpoint
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    /// Authentication token (if required)
    #[clap(long)]
    x_token: Option<String>,

    /// Program ID to monitor (e.g., Raydium AMM: 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8)
    #[clap(short, long)]
    program: String,

    /// Include all accounts from matching transactions (not just filtered ones)
    #[clap(long, default_value_t = true)]
    include_all_accounts: bool,

    /// Filter Token/Token2022 accounts to only include mints (useful for getting transfer fees)
    #[clap(long, default_value_t = true)]
    readonly_mints_only: bool,

    /// Commitment level: processed, confirmed, or finalized
    #[clap(long, default_value_t = String::from("confirmed"))]
    commitment: String,
}

fn parse_commitment(s: &str) -> CommitmentLevel {
    match s.to_lowercase().as_str() {
        "processed" => CommitmentLevel::Processed,
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        _ => {
            warn!("Unknown commitment level '{}', defaulting to confirmed", s);
            CommitmentLevel::Confirmed
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    info!("Connecting to {}", args.endpoint);
    info!("Monitoring program: {}", args.program);
    info!("Include all accounts: {}", args.include_all_accounts);
    info!("Readonly mints only: {}", args.readonly_mints_only);

    // Connect to Yellowstone gRPC
    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    // Create subscription request
    let request = SubscribeRequest {
        // Subscribe to transaction_accounts for our target program
        transaction_accounts: maplit::hashmap! {
            "my_program_monitor".to_owned() => SubscribeRequestFilterTransactionAccounts {
                // Filter by account OWNER - matches accounts whose owner is this program
                // Use this when you want to match all accounts owned by a program
                // Example: Put Token program ID here to get all token/mint accounts
                owner: vec![args.program.clone()],
                // Filter by account PUBKEY - matches specific account addresses
                // Use this when you want to match a specific account (e.g., a DEX pool)
                account: vec![],
                // Include ALL accounts from matching transactions (not just the filtered ones)
                // This gives us the complete picture of what changed in the transaction
                include_all_accounts: Some(args.include_all_accounts),
                // Only include Token/Token2022 mint accounts (not token accounts)
                // Useful for getting mint data like transfer fees without the noise
                readonly_mints_only: Some(args.readonly_mints_only),
            }
        },
        commitment: Some(parse_commitment(&args.commitment) as i32),
        ..Default::default()
    };

    info!("Subscribing with request: {:?}", request);

    // Subscribe
    let (_subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("Subscription active, waiting for transaction accounts updates...");

    // Process incoming messages
    while let Some(message) = stream.next().await {
        let message = message?;

        match message.update_oneof {
            Some(UpdateOneof::TransactionAccounts(update)) => {
                handle_transaction_accounts_update(&update, &message.filters);
            }
            Some(UpdateOneof::Ping(_)) => {
                // Ignore pings
            }
            Some(other) => {
                warn!("Received unexpected update type: {:?}", other);
            }
            None => {
                warn!("Received message with no update");
            }
        }
    }

    Ok(())
}

/// Handle a transaction accounts update
fn handle_transaction_accounts_update(
    update: &SubscribeUpdateTransactionAccounts,
    filters: &[String],
) {
    let signature = bs58::encode(&update.signature).into_string();

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("Transaction Accounts Update");
    info!("  Signature: {}", signature);
    info!("  Slot: {}", update.slot);
    info!("  Index: {}", update.index);
    info!("  Filters matched: {:?}", filters);
    info!("  Accounts ({} total):", update.accounts.len());

    for (i, account) in update.accounts.iter().enumerate() {
        let pubkey = bs58::encode(&account.pubkey).into_string();
        let owner = bs58::encode(&account.owner).into_string();

        // Detect account type for display
        let account_type = detect_account_type(&account.owner, account.data.len());

        info!(
            "    [{}] {} ({} bytes) - {}",
            i,
            pubkey,
            account.data.len(),
            account_type
        );
        info!("        Owner: {}", owner);
        info!(
            "        Lamports: {}, Executable: {}, Rent Epoch: {}",
            account.lamports, account.executable, account.rent_epoch
        );

        // For mint accounts, show additional info
        if account_type.contains("Mint") && account.data.len() >= 82 {
            if let Some(mint_info) = parse_mint_info(&account.data) {
                info!(
                    "        Mint Info: supply={}, decimals={}, initialized={}",
                    mint_info.supply, mint_info.decimals, mint_info.is_initialized
                );
            }
        }
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

/// Detect account type based on owner and data size
fn detect_account_type(owner: &[u8], data_len: usize) -> &'static str {
    const TOKEN_PROGRAM: &[u8] = &[
        0x06, 0xdd, 0xf6, 0xe1, 0xd7, 0x65, 0xa1, 0x93, 0xd9, 0xcb, 0xe1, 0x46, 0xce, 0xeb, 0x79,
        0xac, 0x1c, 0xb4, 0x85, 0xed, 0x5f, 0x5b, 0x37, 0x91, 0x3a, 0x8c, 0xf5, 0x85, 0x7e, 0xff,
        0x00, 0xa9,
    ];
    const TOKEN_2022_PROGRAM: &[u8] = &[
        0x06, 0xdd, 0xf6, 0xe1, 0xd7, 0x65, 0xa1, 0x93, 0xd9, 0xcb, 0xe1, 0x46, 0xce, 0xeb, 0x79,
        0xac, 0x87, 0x7b, 0xa8, 0x4e, 0x82, 0x97, 0x49, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01,
    ];

    if owner == TOKEN_PROGRAM {
        match data_len {
            82 => "Token Mint",
            165 => "Token Account",
            _ => "Token (unknown)",
        }
    } else if owner == TOKEN_2022_PROGRAM {
        match data_len {
            82 => "Token2022 Mint (no extensions)",
            165 => "Token2022 Account (no extensions)",
            _ if data_len > 165 => {
                // Check AccountType discriminator at offset 165
                "Token2022 (with extensions)"
            }
            _ if data_len > 82 => "Token2022 Mint (with extensions)",
            _ => "Token2022 (unknown)",
        }
    } else {
        "Program Account"
    }
}

/// Basic mint info parsed from account data
struct MintInfo {
    supply: u64,
    decimals: u8,
    is_initialized: bool,
}

/// Parse basic mint information from Token/Token2022 mint account data
fn parse_mint_info(data: &[u8]) -> Option<MintInfo> {
    if data.len() < 82 {
        return None;
    }

    // Mint layout (first 82 bytes):
    // - mint_authority (36 bytes): COption<Pubkey>
    // - supply (8 bytes): u64
    // - decimals (1 byte): u8
    // - is_initialized (1 byte): bool
    // - freeze_authority (36 bytes): COption<Pubkey>

    let supply = u64::from_le_bytes(data[36..44].try_into().ok()?);
    let decimals = data[44];
    let is_initialized = data[45] != 0;

    Some(MintInfo {
        supply,
        decimals,
        is_initialized,
    })
}
