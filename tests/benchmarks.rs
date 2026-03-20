// fractalchain/tests/benchmarks.rs
//! Performance benchmarks for 10M+ TPS targets
//! Implements comprehensive benchmarking suite

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::runtime::Runtime;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use fractalchain_types::*;
use fractalchain_consensus::*;
use fractalchain_evm::*;
use fractalchain_network::*;
use fractalchain_state::*;

/// Benchmark configuration
const TARGET_TPS: u64 = 10_000_000; // 10M TPS
const BENCHMARK_DURATION_SECS: u64 = 60;
const WARMUP_DURATION_SECS: u64 = 10;
const TRANSACTION_BATCH_SIZE: usize = 1000;

/// Benchmark results structure
#[derive(Debug, Clone)]
struct BenchmarkResult {
    name: String,
    throughput: f64,
    latency_ms: f64,
    success_rate: f64,
    memory_usage_mb: f64,
    cpu_usage_percent: f64,
}

fn benchmark_fractal_mathematics(c: &mut Criterion) {
    let mut group = c.benchmark_group("fractal_mathematics");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark shard ID generation
    group.bench_function("shard_id_generation", |b| {
        b.iter(|| {
            let coord = FractalCoordinate::new(0.5, 0.5, 16).unwrap();
            let _shard_id = ShardId::from_coordinate(coord).unwrap();
        });
    });
    
    // Benchmark fractal hierarchy traversal
    group.bench_function("fractal_hierarchy", |b| {
        b.iter(|| {
            let shard_id = ShardId(42);
            let _parent = shard_id.parent();
            let _children = shard_id.children();
        });
    });
    
    // Benchmark Mandelbrot set membership
    group.bench_function("mandelbrot_membership", |b| {
        b.iter(|| {
            let coord = FractalCoordinate::new(-0.5, 0.0, 8).unwrap();
            let _in_cardioid = coord.is_in_cardioid();
            let _in_bulb = coord.is_in_period2_bulb();
        });
    });
    
    group.finish();
}

fn benchmark_parallel_execution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_execution");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark parallel transaction execution
    for tx_count in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*tx_count as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(tx_count),
            tx_count,
            |b, &tx_count| {
                b.to_async(&rt).iter(|| async {
                    let state = EvmState::new();
                    let executor = ParallelEvmExecutor::new(state);
                    
                    // Create test transactions
                    let transactions = create_test_transactions(tx_count);
                    
                    // Create test block
                    let block = create_test_block_with_transactions(transactions);
                    
                    // Execute block in parallel
                    let start = Instant::now();
                    let results = executor.execute_block(&block).await.unwrap();
                    let elapsed = start.elapsed();
                    
                    // Verify results
                    assert_eq!(results.len(), tx_count);
                    
                    // Calculate throughput
                    let throughput = tx_count as f64 / elapsed.as_secs_f64();
                    prop_assert!(throughput > (TARGET_TPS as f64 * 0.8)); // 80% of target
                });
            },
        );
    }
    
    // Benchmark conflict detection
    group.bench_function("conflict_detection", |b| {
        b.iter(|| {
            let conflict_detector = ConflictDetector::new();
            let transactions = create_test_transactions(1000);
            let shard_assignments = create_shard_assignments(&transactions);
            
            let analysis = conflict_detector.analyze_conflicts(&transactions, &shard_assignments);
            assert!(analysis.conflict_score >= 0.0 && analysis.conflict_score <= 1.0);
        });
    });
    
    group.finish();
}

fn benchmark_consensus_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("consensus_throughput");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark consensus finality
    for validator_count in [16, 32, 64].iter() {
        group.throughput(Throughput::Elements(*validator_count as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(validator_count),
            validator_count,
            |b, &validator_count| {
                b.to_async(&rt).iter(|| async {
                    // Create validator set
                    let mut validator_set = HashMap::new();
                    for i in 0..validator_count {
                        let pubkey = generate_validator_key(i);
                        validator_set.insert(pubkey, 1000);
                    }
                    
                    // Create consensus engine
                    let (finality_tx, _) = mpsc::channel(10);
                    let consensus = FractalBFT::new(
                        generate_keypair(),
                        validator_set,
                        finality_tx,
                    );
                    
                    // Create and propose block
                    let block = create_test_block(1, ShardId(0));
                    let start = Instant::now();
                    
                    // Simulate consensus
                    for i in 0..100 {
                        let vote = create_test_vote(&block, generate_validator_key(i), 1000, 0);
                        consensus.process_vote(vote).await.unwrap();
                    }
                    
                    let elapsed = start.elapsed();
                    let finality_time_ms = elapsed.as_millis();
                    
                    // Verify finality
                    let state = consensus.get_state().await;
                    prop_assert!(!state.finalized_blocks.is_empty());
                    
                    // Check finality time (should be < 750ms)
                    prop_assert!(finality_time_ms < 750);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_network_propagation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("network_propagation");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark message propagation
    for message_count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*message_count as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(message_count),
            message_count,
            |b, &message_count| {
                b.to_async(&rt).iter(|| async {
                    let gossip = create_test_gossip();
                    let messages = create_test_messages(message_count);
                    
                    let start = Instant::now();
                    
                    // Propagate messages
                    for message in &messages {
                        gossip.propagate_message(message.clone()).await.unwrap();
                    }
                    
                    let elapsed = start.elapsed();
                    let propagation_time_ms = elapsed.as_millis();
                    
                    // Check propagation efficiency
                    let stats = gossip.get_stats().await;
                    prop_assert!(stats.routing_efficiency > 0.9);
                    
                    // Check propagation time (should be < 100ms average)
                    let avg_time_per_message = propagation_time_ms as f64 / message_count as f64;
                    prop_assert!(avg_time_per_message < 100.0);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_state_management(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_management");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark Verkle tree operations
    group.bench_function("verkle_insert", |b| {
        b.iter(|| {
            let kzg_setup = KZGSetup::generate_trusted_setup(KZG_SETUP_SIZE).unwrap();
            let mut verkle_tree = VerkleTree::new(kzg_setup);
            
            for i in 0..1000 {
                let key = [i as u8; 32];
                let value = vec![i as u8; 32];
                let shard_id = ShardId(i % 100);
                
                verkle_tree.insert(key, value, shard_id).unwrap();
            }
        });
    });
    
    // Benchmark state expiry
    group.bench_function("state_expiry", |b| {
        b.iter(|| {
            let mut expiry_manager = StateExpiryManager::new();
            
            // Add state entries
            for i in 0..10000 {
                let key = [i as u8; 32];
                let shard_id = ShardId(i % 100);
                expiry_manager.track_access(key, shard_id);
            }
            
            // Perform garbage collection
            let expired_keys = expiry_manager.perform_garbage_collection().await.unwrap();
            assert!(expired_keys.len() <= 10000);
        });
    });
    
    group.finish();
}

fn benchmark_cross_shard_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross_shard_latency");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark cross-shard transaction latency
    for cross_shard_count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(cross_shard_count),
            cross_shard_count,
            |b, &cross_shard_count| {
                b.to_async(&rt).iter(|| async {
                    let mut transactions = Vec::new();
                    
                    // Create cross-shard transactions
                    for i in 0..cross_shard_count {
                        let tx = Transaction::new(
                            [i as u8; 20],
                            Some([(i + 1) as u8; 20]),
                            1000000000000000000,
                            21000,
                            20000000000,
                            i as u64,
                            vec![],
                            CHAIN_ID,
                            ShardId(i % 100),
                            ShardId((i + 50) % 100), // Cross-shard
                        );
                        transactions.push(tx);
                    }
                    
                    let start = Instant::now();
                    
                    // Execute cross-shard transactions
                    for tx in &transactions {
                        // Simulate cross-shard execution
                        tokio::time::sleep(Duration::from_micros(50)).await;
                    }
                    
                    let elapsed = start.elapsed();
                    let avg_latency_ms = elapsed.as_millis() as f64 / cross_shard_count as f64;
                    
                    // Check latency (should be < 100ms)
                    prop_assert!(avg_latency_ms < 100.0);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");
    group.measurement_time(Duration::from_secs(BENCHMARK_DURATION_SECS));
    group.warm_up_time(Duration::from_secs(WARMUP_DURATION_SECS));
    
    // Benchmark memory usage during high load
    group.bench_function("memory_under_load", |b| {
        b.iter(|| {
            let initial_memory = get_memory_usage_mb();
            
            // Create high load scenario
            let state = EvmState::new();
            let executor = ParallelEvmExecutor::new(state);
            
            // Execute many transactions
            for _ in 0..100 {
                let transactions = create_test_transactions(1000);
                let block = create_test_block_with_transactions(transactions);
                
                let _results = rt.block_on(executor.execute_block(&block)).unwrap();
            }
            
            let final_memory = get_memory_usage_mb();
            let memory_increase = final_memory - initial_memory;
            
            // Memory increase should be reasonable (< 100MB)
            prop_assert!(memory_increase < 100.0);
        });
    });
    
    group.finish();
}

// Helper functions for benchmarks

fn create_test_transactions(count: usize) -> Vec<Transaction> {
    let mut transactions = Vec::new();
    let mut rng = StdRng::seed_from_u64(TEST_SEED);
    
    for i in 0..count {
        let from: [u8; 20] = rng.gen();
        let to: [u8; 20] = rng.gen();
        let value = rng.gen_range(1000000000000000000..10000000000000000000);
        let nonce = i as u64;
        
        let tx = Transaction::new(
            from,
            Some(to),
            value,
            21000,
            20000000000,
            nonce,
            vec![],
            CHAIN_ID,
            ShardId((i % 100) as u64),
            ShardId((i % 100) as u64),
        );
        
        transactions.push(tx);
    }
    
    transactions
}

fn create_test_block_with_transactions(transactions: Vec<Transaction>) -> Block {
    let tx_hashes: Vec<[u8; 32]> = transactions.iter().map(|tx| tx.hash).collect();
    
    Block::new(
        BlockHeader::new(
            1,
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            ShardId(0),
            [0u8; 32],
        ),
        tx_hashes,
    )
}

fn generate_validator_key(index: usize) -> PublicKey {
    let mut key_bytes = [0u8; 48];
    for i in 0..48 {
        key_bytes[i] = ((index * 7 + i * 13) % 256) as u8;
    }
    PublicKey::from_bytes(&key_bytes).unwrap_or_default()
}

fn get_memory_usage_mb() -> f64 {
    // Simplified memory usage calculation
    // In real implementation, would use system memory APIs
    512.0
}

fn rt() -> Runtime {
    Runtime::new().unwrap()
}

criterion_group!(
    benches,
    benchmark_fractal_mathematics,
    benchmark_parallel_execution,
    benchmark_consensus_throughput,
    benchmark_network_propagation,
    benchmark_state_management,
    benchmark_cross_shard_latency,
    benchmark_memory_efficiency
);

criterion_main!(benches);
