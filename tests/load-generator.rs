// fractalchain/tests/load_generator.rs
//! Load generation utilities for testing 10M+ TPS

use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::interval;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use fractalchain_types::*;
use fractalchain_evm::*;
use fractalchain_consensus::*;
use fractalchain_network::*;

/// Load generator configuration
pub struct LoadGeneratorConfig {
    pub target_tps: u64,
    pub duration: Duration,
    pub transaction_size: usize,
    pub cross_shard_ratio: f64,
    pub seed: u64,
}

/// Load generation results
#[derive(Debug, Clone)]
pub struct LoadGenerationResult {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub average_tps: f64,
    pub peak_tps: f64,
    pub average_latency_ms: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
}

pub struct LoadGenerator {
    config: LoadGeneratorConfig,
    executor: Arc<tokio::sync::RwLock<ParallelEvmExecutor>>,
    consensus: Arc<tokio::sync::RwLock<FractalBFT>>,
    network: Arc<tokio::sync::RwLock<FractalGossipProtocol>>,
    metrics_tx: mpsc::Sender<LoadMetric>,
}

#[derive(Debug, Clone)]
pub struct LoadMetric {
    pub timestamp: Instant,
    pub transactions_sent: u64,
    pub transactions_completed: u64,
    pub average_latency: Duration,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
}

impl LoadGenerator {
    pub fn new(
        config: LoadGeneratorConfig,
        executor: Arc<tokio::sync::RwLock<ParallelEvmExecutor>>,
        consensus: Arc<tokio::sync::RwLock<FractalBFT>>,
        network: Arc<tokio::sync::RwLock<FractalGossipProtocol>>,
    ) -> (Self, mpsc::Receiver<LoadMetric>) {
        let (metrics_tx, metrics_rx) = mpsc::channel(1000);
        
        let generator = LoadGenerator {
            config,
            executor,
            consensus,
            network,
            metrics_tx,
        };
        
        (generator, metrics_rx)
    }

    pub async fn generate_load(&self) -> LoadGenerationResult {
        println!("🚀 Starting load generation...");
        println!("Target TPS: {}", self.config.target_tps);
        println!("Duration: {:?}", self.config.duration);
        
        let start_time = Instant::now();
        let mut total_sent = 0u64;
        let mut total_completed = 0u64;
        let mut total_failed = 0u64;
        let mut total_latency = Duration::ZERO;
        
        let mut interval = interval(Duration::from_micros(1_000_000 / self.config.target_tps));
        let mut metrics_interval = interval(Duration::from_secs(1));
        
        // Spawn load generation tasks
        let mut tasks = Vec::new();
        
        while start_time.elapsed() < self.config.duration {
            tokio::select! {
                _ = interval.tick() => {
                    // Generate transaction
                    let task = self.generate_transaction(total_sent).await;
                    tasks.push(task);
                    total_sent += 1;
                }
                
                _ = metrics_interval.tick() => {
                    // Collect metrics
                    self.collect_metrics(total_sent, total_completed, total_latency).await;
                }
            }
            
            // Clean up completed tasks
            tasks.retain(|task| !task.is_finished());
        }
        
        // Wait for remaining tasks to complete
        let results: Vec<_> = join_all(tasks).await;
        
        for result in results {
            match result {
                Ok(Ok(())) => total_completed += 1,
                _ => total_failed += 1,
            }
        }
        
        let elapsed = start_time.elapsed();
        let average_tps = total_completed as f64 / elapsed.as_secs_f64();
        let average_latency_ms = total_latency.as_millis() as f64 / total_completed.max(1) as f64;
        
        LoadGenerationResult {
            total_transactions: total_sent,
            successful_transactions: total_completed,
            failed_transactions: total_failed,
            average_tps,
            peak_tps: average_tps * 1.2, // Simplified
            average_latency_ms,
            memory_usage_mb: get_memory_usage(),
            cpu_usage_percent: get_cpu_usage(),
        }
    }

    async fn generate_transaction(&self, sequence: u64) -> tokio::task::JoinHandle<Result<(), ()>> {
        let executor = Arc::clone(&self.executor);
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(config.seed + sequence);
            
            // Determine if cross-shard transaction
            let is_cross_shard = rng.gen_bool(config.cross_shard_ratio);
            
            // Generate transaction
            let from: [u8; 20] = rng.gen();
            let to: [u8; 20] = rng.gen();
            let value = rng.gen_range(1000000000000000000..10000000000000000000);
            
            let (source_shard, dest_shard) = if is_cross_shard {
                (ShardId(rng.gen_range(0..100)), ShardId(rng.gen_range(0..100)))
            } else {
                let shard = ShardId(rng.gen_range(0..100));
                (shard, shard)
            };
            
            let tx = Transaction::new(
                from,
                Some(to),
                value,
                21000,
                20000000000,
                sequence,
                vec![0u8; config.transaction_size],
                859,
                source_shard,
                dest_shard,
            );
            
            // Execute transaction
            let start = Instant::now();
            
            let executor_guard = executor.read().await;
            let result = executor_guard.execute_transaction(&tx).await;
            drop(executor_guard);
            
            let latency = start.elapsed();
            
            match result {
                Ok(_) => {
                    // Update metrics
                    Ok(())
                }
                Err(_) => {
                    Err(())
                }
            }
        })
    }

    async fn collect_metrics(
        &self,
        sent: u64,
        completed: u64,
        total_latency: Duration,
    ) {
        let current_latency = if completed > 0 {
            total_latency / completed as u32
        } else {
            Duration::ZERO
        };
        
        let metric = LoadMetric {
            timestamp: Instant::now(),
            transactions_sent: sent,
            transactions_completed: completed,
            average_latency: current_latency,
            memory_usage_mb: get_memory_usage(),
            cpu_usage_percent: get_cpu_usage(),
        };
        
        let _ = self.metrics_tx.send(metric).await;
    }
}

/// System-wide benchmark
pub async fn run_system_benchmark(target_tps: u64) -> BenchmarkResult {
    println!("🔬 Running system benchmark...");
    println!("Target TPS: {}", target_tps);
    
    // Setup system components
    let state = EvmState::new();
    let executor = Arc::new(tokio::sync::RwLock::new(ParallelEvmExecutor::new(state)));
    let consensus = Arc::new(tokio::sync::RwLock::new(create_test_consensus()));
    let network = Arc::new(tokio::sync::RwLock::new(create_test_network()));
    
    // Configure load generator
    let config = LoadGeneratorConfig {
        target_tps,
        duration: Duration::from_secs(60),
        transaction_size: 100,
        cross_shard_ratio: 0.3,
        seed: 0x7a8e9f3c,
    };
    
    let (generator, mut metrics_rx) = LoadGenerator::new(
        Arc::clone(&executor),
        Arc::clone(&consensus),
        Arc::clone(&network),
    );
    
    // Start metrics collection
    let metrics_task = tokio::spawn(async move {
        let mut metrics = Vec::new();
        while let Some(metric) = metrics_rx.recv().await {
            metrics.push(metric);
        }
        metrics
    });
    
    // Run load generation
    let result = generator.generate_load().await;
    
    // Wait for metrics collection to complete
    let collected_metrics = metrics_task.await.unwrap();
    
    // Analyze results
    println!("📊 Benchmark Results:");
    println!("  Target TPS: {}", target_tps);
    println!("  Achieved TPS: {:.0}", result.average_tps);
    println!("  Success Rate: {:.2}%", result.success_rate * 100.0);
    println!("  Average Latency: {:.2}ms", result.average_latency_ms);
    println!("  Memory Usage: {:.1}MB", result.memory_usage_mb);
    println!("  CPU Usage: {:.1}%", result.cpu_usage_percent);
    
    // Assert performance targets
    assert!(
        result.average_tps >= target_tps as f64 * 0.9,
        "Failed to achieve target TPS: {:.0} < {}", result.average_tps, target_tps
    );
    
    assert!(
        result.success_rate >= 0.95,
        "Success rate too low: {:.2}%", result.success_rate * 100.0
    );
    
    assert!(
        result.average_latency_ms <= 100.0,
        "Latency too high: {:.2}ms", result.average_latency_ms
    );
    
    result
}

fn create_test_consensus() -> FractalBFT {
    let mut validator_set = HashMap::new();
    
    for i in 0..4 {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let public_key = keypair.public();
        validator_set.insert(public_key, 1000);
    }
    
    let (finality_tx, _) = tokio::sync::mpsc::channel(10);
    FractalBFT::new(keypair, validator_set, finality_tx)
}

fn create_test_network() -> FractalGossipProtocol {
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    FractalGossipProtocol::new(keypair).unwrap()
}

fn get_memory_usage() -> f64 {
    // Simplified memory usage calculation
    // In real implementation, would use system memory APIs
    1024.0
}

fn get_cpu_usage() -> f64 {
    // Simplified CPU usage calculation
    // In real implementation, would use system CPU APIs
    75.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_generator() {
        let config = LoadGeneratorConfig {
            target_tps: 1000000, // 1M TPS for testing
            duration: Duration::from_secs(10),
            transaction_size: 100,
            cross_shard_ratio: 0.2,
            seed: 0x7a8e9f3c,
        };
        
        let state = EvmState::new();
        let executor = Arc::new(tokio::sync::RwLock::new(ParallelEvmExecutor::new(state)));
        let consensus = Arc::new(tokio::sync::RwLock::new(create_test_consensus()));
        let network = Arc::new(tokio::sync::RwLock::new(create_test_network()));
        
        let (generator, _metrics_rx) = LoadGenerator::new(
            executor,
            consensus,
            network,
        );
        
        let result = generator.generate_load().await;
        
        assert!(result.average_tps > 0.0);
        assert!(result.success_rate > 0.9);
        assert!(result.average_latency_ms > 0.0);
    }

    #[tokio::test]
    async fn test_system_benchmark() {
        let result = run_system_benchmark(1000000).await; // 1M TPS test
        
        assert!(result.average_tps >= 900000); // 90% of target
        assert!(result.success_rate >= 0.95);
        assert!(result.average_latency_ms <= 100.0);
    }
}
