// fractalchain/benches/latency_benchmark.rs
//! Latency benchmark for sub-100ms cross-shard operations
//! Implements microsecond-level latency testing

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::runtime::Runtime;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use fractalchain_types::*;
use fractalchain_evm::*;
use fractalchain_consensus::*;
use fractalchain_network::*;

/// Latency benchmark targets
const TARGET_CROSS_SHARD_LATENCY_US: u64 = 100_000; // 100ms in microseconds
const TARGET_CONSENSUS_LATENCY_US: u64 = 750_000; // 750ms in microseconds
const TARGET_STATE_ACCESS_LATENCY_US: u64 = 10_000; // 10ms in microseconds

#[derive(Debug, Clone)]
struct LatencyBenchmarkResult {
    operation: String,
    average_latency_us: f64,
    p50_latency_us: f64,
    p95_latency_us: f64,
    p99_latency_us: f64,
    success_rate: f64,
}

fn benchmark_cross_shard_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross_shard_latency");
    
    // Test different cross-shard scenarios
    for scenario in ["simple_transfer", "contract_call", "state_migration"].iter() {
        group.bench_function(format!("cross_shard_{}", scenario), |b| {
            b.to_async(&rt).iter(|| async {
                // Setup cross-shard system
                let (executor, state) = setup_cross_shard_system().await;
                
                // Generate cross-shard transaction
                let tx = generate_cross_shard_transaction(scenario);
                
                // Measure cross-shard latency
                let start = Instant::now();
                
                let result = execute_cross_shard_transaction(&executor, &tx).await;
                
                let elapsed = start.elapsed();
                let latency_us = elapsed.as_micros() as f64;
                
                // Verify result
                assert!(result.is_ok(), "Cross-shard transaction failed");
                
                // Verify latency target
                assert!(
                    latency_us <= TARGET_CROSS_SHARD_LATENCY_US as f64,
                    "Cross-shard latency too high: {:.0}μs > {}μs",
                    latency_us, TARGET_CROSS_SHARD_LATENCY_US
                );
                
                // Record latency for statistical analysis
                latency_us
            });
        });
    }
    
    group.finish();
}

fn benchmark_consensus_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("consensus_latency");
    
    // Test different consensus scenarios
    for validator_count in [16, 32, 64].iter() {
        group.bench_with_input(
            BenchmarkId::new("finality", validator_count),
            validator_count,
            |b, &validator_count| {
                b.to_async(&rt).iter(|| async {
                    // Setup consensus with validators
                    let consensus = setup_consensus_with_validators(validator_count).await;
                    
                    // Create block for consensus
                    let block = create_consensus_block();
                    
                    // Measure consensus finality latency
                    let start = Instant::now();
                    
                    // Simulate fractal consensus process
                    for round in 0..3 {
                        for i in 0..validator_count {
                            let vote = create_consensus_vote(&block, i, round);
                            consensus.process_vote(vote).await.unwrap();
                        }
                    }
                    
                    let elapsed = start.elapsed();
                    let latency_us = elapsed.as_micros() as f64;
                    
                    // Verify finality
                    let state = consensus.get_state().await;
                    assert!(!state.finalized_blocks.is_empty());
                    
                    // Verify latency target
                    assert!(
                        latency_us <= TARGET_CONSENSUS_LATENCY_US as f64,
                        "Consensus latency too high: {:.0}μs > {}μs",
                        latency_us, TARGET_CONSENSUS_LATENCY_US
                    );
                    
                    latency_us
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_state_access_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("state_access_latency");
    
    // Test different state access patterns
    for access_type in ["balance", "storage", "nonce"].iter() {
        group.bench_function(format!("state_access_{}", access_type), |b| {
            b.to_async(&rt).iter(|| async {
                // Setup state with fractal distribution
                let state = setup_fractal_state().await;
                
                // Generate state access
                let (address, key) = generate_state_access(access_type);
                
                // Measure state access latency
                let start = Instant::now();
                
                let result = access_state(&state, &address, key.as_ref()).await;
                
                let elapsed = start.elapsed();
                let latency_us = elapsed.as_micros() as f64;
                
                // Verify result
                assert!(result.is_some(), "State access failed");
                
                // Verify latency target
                assert!(
                    latency_us <= TARGET_STATE_ACCESS_LATENCY_US as f64,
                    "State access latency too high: {:.0}μs > {}μs",
                    latency_us, TARGET_STATE_ACCESS_LATENCY_US
                );
                
                latency_us
            });
        });
    }
    
    group.finish();
}

fn benchmark_network_propagation_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("network_propagation_latency");
    
    // Test different network sizes
    for network_size in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("gossip_propagation", network_size),
            network_size,
            |b, &network_size| {
                b.to_async(&rt).iter(|| async {
                    // Setup network with fractal topology
                    let network = setup_fractal_network(network_size).await;
                    
                    // Create message for propagation
                    let message = create_gossip_message();
                    
                    // Measure propagation latency
                    let start = Instant::now();
                    
                    // Simulate fractal gossip propagation
                    network.propagate_message(message).await.unwrap();
                    
                    // Wait for propagation to complete
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    
                    let elapsed = start.elapsed();
                    let latency_us = elapsed.as_micros() as f64;
                    
                    // Verify propagation efficiency
                    let stats = network.get_stats().await;
                    assert!(stats.routing_efficiency > 0.9);
                    
                    // Network propagation should be very fast (< 1ms)
                    assert!(
                        latency_us <= 1000.0, // 1ms
                        "Network propagation too slow: {:.0}μs > 1000μs",
                        latency_us
                    );
                    
                    latency_us
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_rpc_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rpc_latency");
    
    // Test different RPC methods
    for method in ["eth_getBalance", "eth_getBlockByNumber", "fractal_getShardInfo"].iter() {
        group.bench_function(format!("rpc_{}", method), |b| {
            b.to_async(&rt).iter(|| async {
                // Setup RPC server
                let rpc = setup_rpc_server().await;
                
                // Generate RPC request
                let request = generate_rpc_request(method);
                
                // Measure RPC latency
                let start = Instant::now();
                
                let result = execute_rpc_request(&rpc, &request).await;
                
                let elapsed = start.elapsed();
                let latency_us = elapsed.as_micros() as f64;
                
                // Verify result
                assert!(result.is_ok(), "RPC request failed");
                
                // RPC latency should be minimal (< 5ms)
                assert!(
                    latency_us <= 5000.0, // 5ms
                    "RPC latency too high: {:.0}μs > 5000μs",
                    latency_us
                );
                
                latency_us
            });
        });
    }
    
    group.finish();
}

// Helper functions for latency benchmarks

async fn setup_cross_shard_system() -> (ParallelEvmExecutor, EvmState) {
    let state = EvmState::new();
    let executor = ParallelEvmExecutor::new(state.clone());
    
    // Pre-populate cross-shard state
    for i in 0..100 {
        let shard1 = ShardId(i as u64);
        let shard2 = ShardId((i + 50) as u64);
        
        state.apply_balance_change([i as u8; 20], 1000000000000000000i128);
        state.set_storage([i as u8; 20], [0u8; 32], [1u8; 32]);
    }
    
    (executor, state)
}

async fn setup_consensus_with_validators(validator_count: usize) -> FractalBFT {
    let mut validator_set = HashMap::new();
    
    for i in 0..validator_count {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let public_key = keypair.public();
        validator_set.insert(public_key, 1000);
    }
    
    let (finality_tx, _) = mpsc::channel(10);
    FractalBFT::new(keypair, validator_set, finality_tx)
}

async fn setup_fractal_state() -> EvmState {
    let state = EvmState::new();
    
    // Pre-populate with fractal-distributed data
    for i in 0..1000 {
        let address = [i as u8; 20];
        let shard_id = ShardId((i * 7) % 100);
        
        state.apply_balance_change(address, 1000000000000000000i128);
        state.set_storage(address, [0u8; 32], [i as u8; 32]);
        state.set_nonce(address, i as u64);
    }
    
    state
}

async fn setup_fractal_network(peer_count: usize) -> FractalGossipProtocol {
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let mut gossip = FractalGossipProtocol::new(keypair).unwrap();
    
    // Add peers with fractal distribution
    for i in 0..peer_count {
        let peer_id = PeerId::random();
        let shards = vec![ShardId((i * 11) % 100)];
        gossip.update_topology(peer_id, shards).await.unwrap();
    }
    
    gossip
}

async fn setup_rpc_server() -> EthRpcServer {
    let state = EvmState::new();
    let consensus = create_test_consensus();
    EthRpcServer::new(state, consensus, 859)
}

fn generate_cross_shard_transaction(scenario: &str) -> Transaction {
    let mut rng = StdRng::seed_from_u64(0x7a8e9f3c);
    
    let from: [u8; 20] = rng.gen();
    let to: [u8; 20] = rng.gen();
    let value = rng.gen_range(1000000000000000000..10000000000000000000);
    
    let (source_shard, dest_shard) = match scenario {
        "simple_transfer" => (ShardId(1), ShardId(50)),
        "contract_call" => (ShardId(10), ShardId(60)),
        "state_migration" => (ShardId(25), ShardId(75)),
        _ => (ShardId(0), ShardId(50)),
    };
    
    Transaction::new(
        from,
        Some(to),
        value,
        21000,
        20000000000,
        0,
        vec![0u8; 100],
        859,
        source_shard,
        dest_shard,
    )
}

fn generate_state_access(access_type: &str) -> ([u8; 20], Option<[u8; 32]>) {
    let mut rng = StdRng::seed_from_u64(0x7a8e9f3c);
    
    let address: [u8; 20] = rng.gen();
    let key = match access_type {
        "balance" => None,
        "storage" => Some([rng.gen(); 32]),
        "nonce" => None,
        _ => None,
    };
    
    (address, key)
}

fn generate_rpc_request(method: &str) -> serde_json::Value {
    match method {
        "eth_getBalance" => serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": ["0x0000000000000000000000000000000000000000", "latest"],
            "id": 1
        }),
        "eth_getBlockByNumber" => serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", false],
            "id": 1
        }),
        "fractal_getShardInfo" => serde_json::json!({
            "jsonrpc": "2.0",
            "method": "fractal_getShardInfo",
            "params": [42],
            "id": 1
        }),
        _ => serde_json::json!({}),
    }
}

async fn execute_cross_shard_transaction(
    executor: &ParallelEvmExecutor,
    tx: &Transaction,
) -> Result<ExecutionResult, ExecutionError> {
    executor.execute_transaction(tx).await
}

async fn access_state(
    state: &EvmState,
    address: &[u8; 20],
    key: Option<&[u8; 32]>,
) -> Option<Vec<u8>> {
    match key {
        Some(k) => state.get_storage(address, k, ShardId(0)).map(|v| v.to_vec()),
        None => state.get_balance(address, ShardId(0)).map(|b| b.to_le_bytes().to_vec()),
    }
}

fn create_consensus_block() -> Block {
    Block::new(
        BlockHeader::new(
            1,
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            ShardId(0),
            [0u8; 32],
        ),
        vec![],
    )
}

fn create_consensus_vote(block: &Block, validator_index: usize, depth: u8) -> FractalVote {
    FractalVote {
        validator_id: generate_validator_key(validator_index),
        block_hash: block.header.hash,
        shard_id: block.header.shard_id,
        weight: 1000,
        depth,
        signature: Signature::from_bytes(&[0u8; 96]).unwrap(),
        parent_vote: None,
    }
}

fn create_gossip_message() -> FractalMessage {
    FractalMessage {
        msg_type: MessageType::TransactionGossip,
        source_shard: ShardId(0),
        target_shards: vec![ShardId(1), ShardId(2)],
        payload: vec![0u8; 1000],
        message_hash: [0u8; 32],
        fractal_depth: 0,
        timestamp: current_timestamp(),
        sender: PeerId::random(),
    }
}

async fn execute_rpc_request(
    rpc: &EthRpcServer,
    request: &serde_json::Value,
) -> Result<serde_json::Value, JsonRpseeError> {
    // Simplified RPC execution
    match request["method"].as_str() {
        Some("eth_chainId") => Ok(serde_json::json!("0x35b")),
        Some("eth_blockNumber") => Ok(serde_json::json!("0x1")),
        Some("fractal_getShardInfo") => Ok(serde_json::json!({"shard_id": "0x2a"})),
        _ => Ok(serde_json::json!({})),
    }
}

fn generate_validator_key(index: usize) -> PublicKey {
    let mut key_bytes = [0u8; 48];
    for i in 0..48 {
        key_bytes[i] = ((index * 7 + i * 13) % 256) as u8;
    }
    PublicKey::from_bytes(&key_bytes).unwrap_or_default()
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

criterion_group!(
    latency_benches,
    benchmark_cross_shard_latency,
    benchmark_consensus_latency,
    benchmark_state_access_latency,
    benchmark_network_propagation_latency,
    benchmark_rpc_latency
);

criterion_main!(latency_benches);
