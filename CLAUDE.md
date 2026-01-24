# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build the entire workspace
cargo build --release

# Build just the geyser plugin (produces .so library)
cargo build --release -p yellowstone-grpc-geyser

# Check config file validity
cargo run --bin config-check -- --config yellowstone-grpc-geyser/config.json

# Format code
cargo fmt

# Lint
cargo clippy

# Run tests for specific package
cargo test -p yellowstone-grpc-proto
cargo test -p yellowstone-grpc-client

# Run benchmarks (requires plugin-bench feature)
cargo bench -p yellowstone-grpc-proto --features plugin-bench
```

## Architecture

This is **Yellowstone Dragon's Mouth** - a Geyser-based gRPC interface for Solana validators. It streams real-time blockchain data (slots, blocks, transactions, account updates) via gRPC.

### Crate Structure

- **yellowstone-grpc-geyser**: The Solana Geyser plugin (compiles to `libyellowstone_grpc_geyser.so`)
  - `plugin.rs`: Implements `GeyserPlugin` trait - entry point receiving validator events
  - `grpc.rs`: gRPC server implementation, handles subscriptions and message filtering
  - `config.rs`: Plugin configuration parsing and validation
  - `metrics.rs`: Prometheus metrics endpoint

- **yellowstone-grpc-proto**: Protobuf definitions and generated code
  - `proto/geyser.proto`: Main gRPC service definition with Subscribe, Ping, GetLatestBlockhash, etc.
  - `src/plugin/`: Message types and filtering logic for the plugin
  - `src/plugin/filter/`: Subscription filter implementation (accounts, transactions, blocks)
  - Features: `convert` (Solana type conversions), `plugin` (geyser plugin support), `tonic` (gRPC)

- **yellowstone-grpc-client**: Rust client library for connecting to the gRPC server
  - `GeyserGrpcClient`: Main client with subscribe/health/RPC methods
  - `GeyserGrpcBuilder`: Builder pattern for configuring connections

- **examples/rust**: Example client implementations showing subscription patterns

### Data Flow

1. Solana validator calls `GeyserPlugin` methods (`update_account`, `notify_transaction`, etc.)
2. Plugin converts events to internal `Message` types and sends to gRPC service via channels
3. `GrpcService` applies subscription filters and broadcasts matching updates to connected clients
4. Clients receive `SubscribeUpdate` messages via streaming gRPC

### Key Concepts

- **Commitment levels**: processed/confirmed/finalized - filter data by confirmation status
- **Filter limits**: Configurable in `config.json` to restrict subscription scope (max accounts, rejected pubkeys)
- **Block reconstruction**: Plugin collects transactions and accounts to build full block messages
- **x-token**: Optional authentication header for client connections

## Running with Validator

```bash
solana-validator --geyser-plugin-config yellowstone-grpc-geyser/config.json
```

## Proto Regeneration

Proto files are in `yellowstone-grpc-proto/proto/`. The build script (`build.rs`) generates Rust code on compile.
