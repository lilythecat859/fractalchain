// fractalchain/src/lib.rs
//! FRACTALCHAIN - The world's fastest, cheapest, most scalable L1 blockchain
//! 
//! Implements recursive fractal sharding for infinite scalability,
//! parallel EVM execution for 10M+ TPS, and sub-second finality.

pub mod types {
    pub use fractalchain_types::*;
}

pub mod consensus {
    pub use fractalchain_consensus::*;
}

pub mod network {
    pub use fractalchain_network::*;
}

pub mod evm {
    pub use fractalchain_evm::*;
}

pub mod state {
    pub use fractalchain_state::*;
}

pub mod rpc {
    pub use fractalchain_rpc::*;
}

// Re-export core types for convenience
pub use types::{
    ShardId, Block, BlockHeader, Transaction, TransactionReceipt, FractalError,
    FractalCoordinate, HAUSDORFF_DIMENSION, MAX_FRACTAL_DEPTH, SHARD_BASE,
};

pub use consensus::{FractalBFT, FractalVote, ConsensusError};

pub use evm::{ParallelEvmExecutor, ExecutionResult, ExecutionError};

pub use network::{FractalGossipProtocol, FractalDiscovery, NetworkError};

pub use state::{VerkleTree, StateExpiryManager, VerkleProof};

pub use rpc::{EthRpcServer, EthApiServer, FractalApiServer};

/// Genesis configuration
pub const GENESIS_TIMESTAMP: u64 = 1776000000; // February 15, 2026 00:00:00 UTC
pub const CHAIN_ID: u64 = 859; // Homage to lilythecat859
pub const TARGET_TPS: u64 = 10_000_000; // 10M TPS target
pub const BLOCK_TIME_MS: u64 = 250; // 250ms block time
pub const FINALITY_TIMEOUT_MS: u64 = 750; // 750ms finality

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("GIT_COMMIT_HASH");
pub const BUILD_DATE: &str = env!("BUILD_DATE");

/// Performance metrics
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub current_tps: f64,
    pub peak_tps: f64,
    pub average_latency_ms: f64,
    pub finality_time_ms: f64,
    pub cross_shard_latency_ms: f64,
    pub fractal_efficiency: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
}

impl PerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        PerformanceMetrics {
            current_tps: 0.0,
            peak_tps: 0.0,
            average_latency_ms: 0.0,
            finality_time_ms: 0.0,
            cross_shard_latency_ms: 0.0,
            fractal_efficiency: 0.0,
            memory_usage_mb: 0.0,
            cpu_usage_percent: 0.0,
        }
    }

    /// Check if performance targets are met
    pub fn targets_met(&self) -> bool {
        self.current_tps >= TARGET_TPS as f64 * 0.9 && // 90% of target
        self.finality_time_ms <= FINALITY_TIMEOUT_MS as f64 &&
        self.cross_shard_latency_ms <= 100.0 &&
        self.fractal_efficiency >= 0.9
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Node configuration
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Node identity
    pub node_id: String,
    /// Network configuration
    pub network: NetworkConfig,
    /// Consensus configuration
    pub consensus: ConsensusConfig,
    /// State configuration
    pub state: StateConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Enable discovery
    pub enable_discovery: bool,
    /// Maximum peers per shard
    pub max_peers_per_shard: usize,
    /// Cross-shard latency target (ms)
    pub cross_shard_latency_target_ms: u64,
    /// Message propagation timeout (ms)
    pub propagation_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Enable mining
    pub enable_mining: bool,
    /// Mining threads
    pub mining_threads: usize,
    /// Block proposal timeout (ms)
    pub proposal_timeout_ms: u64,
    /// Vote aggregation timeout (ms)
    pub vote_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct StateConfig {
    /// Enable state expiry
    pub enable_state_expiry: bool,
    /// State expiry time (seconds)
    pub state_expiry_time_secs: u64,
    /// Enable Verkle trees
    pub enable_verkle_trees: bool,
}

#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Target TPS
    pub target_tps: u64,
    /// Maximum latency (ms)
    pub max_latency_ms: u64,
    /// Enable performance monitoring
    pub enable_monitoring: bool,
    /// Performance window size
    pub performance_window_size: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            node_id: format!("fractal-node-{}", uuid::Uuid::new_v4()),
            network: NetworkConfig {
                enable_discovery: true,
                max_peers_per_shard: 32,
                cross_shard_latency_target_ms: 100,
                propagation_timeout_ms: 1000,
            },
            consensus: ConsensusConfig {
                enable_mining: false,
                mining_threads: 4,
                proposal_timeout_ms: 1000,
                vote_timeout_ms: 500,
            },
            state: StateConfig {
                enable_state_expiry: true,
                state_expiry_time_secs: 365 * 24 * 60 * 60, // 1 year
                enable_verkle_trees: true,
            },
            performance: PerformanceConfig {
                target_tps: TARGET_TPS,
                max_latency_ms: 100,
                enable_monitoring: true,
                performance_window_size: 1000,
            },
        }
    }
}

/// System information
#[derive(Debug, Clone)]
pub struct SystemInfo {
    /// Version
    pub version: String,
    /// Git commit
    pub git_commit: String,
    /// Build date
    pub build_date: String,
    /// Chain ID
    pub chain_id: u64,
    /// Genesis timestamp
    pub genesis_timestamp: u64,
    /// Fractal depth
    pub fractal_depth: u8,
    /// Total shards
    pub total_shards: u64,
}

impl SystemInfo {
    /// Get system information
    pub fn new() -> Self {
        SystemInfo {
            version: VERSION.to_string(),
            git_commit: GIT_COMMIT.to_string(),
            build_date: BUILD_DATE.to_string(),
            chain_id: CHAIN_ID,
            genesis_timestamp: GENESIS_TIMESTAMP,
            fractal_depth: MAX_FRACTAL_DEPTH,
            total_shards: SHARD_BASE,
        }
    }
}

impl Default for SystemInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions
pub mod utils {
    use super::*;
    use sha2::{Sha256, Digest};
    
    /// Calculate SHA-256 hash
    pub fn hash(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }
    
    /// Convert bytes to hex string
    pub fn to_hex(bytes: &[u8]) -> String {
        hex::encode(bytes)
    }
    
    /// Convert hex string to bytes
    pub fn from_hex(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
        hex::decode(hex)
    }
    
    /// Get current timestamp in milliseconds
    pub fn current_timestamp_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
    
    /// Get current timestamp in seconds
    pub fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
    
    /// Format duration for display
    pub fn format_duration(duration: Duration) -> String {
        if duration < Duration::from_millis(1) {
            format!("{:.2}μs", duration.as_micros())
        } else if duration < Duration::from_secs(1) {
            format!("{:.2}ms", duration.as_millis())
        } else {
            format!("{:.2}s", duration.as_secs())
        }
    }
    
    /// Calculate percentage
    pub fn calculate_percentage(part: f64, total: f64) -> f64 {
        if total == 0.0 {
            0.0
        } else {
            (part / total) * 100.0
        }
    }
}

/// Error handling
pub mod errors {
    use thiserror::Error;
    
    #[derive(Error, Debug)]
    pub enum FractalChainError {
        #[error("Configuration error: {0}")]
        ConfigError(String),
        
        #[error("Network error: {0}")]
        NetworkError(#[from] crate::network::NetworkError),
        
        #[error("Consensus error: {0}")]
        ConsensusError(#[from] crate::consensus::ConsensusError),
        
        #[error("Execution error: {0}")]
        ExecutionError(#[from] crate::evm::ExecutionError),
        
        #[error("State error: {0}")]
        StateError(#[from] crate::types::FractalError),
        
        #[error("RPC error: {0}")]
        RpcError(String),
        
        #[error("Performance error: {0}")]
        PerformanceError(String),
    }
}

/// Logging and monitoring
pub mod logging {
    use tracing::{info, warn, error, debug};
    use super::*;
    
    /// Initialize logging
    pub fn init_logging(level: &str) {
        let filter = match level {
            "trace" => tracing::Level::TRACE,
            "debug" => tracing::Level::DEBUG,
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        };
        
        tracing_subscriber::fmt()
            .with_max_level(filter)
            .init();
    }
    
    /// Log performance metrics
    pub fn log_performance_metrics(metrics: &PerformanceMetrics) {
        info!("Performance Metrics:");
        info!("  Current TPS: {:.0}", metrics.current_tps);
        info!("  Peak TPS: {:.0}", metrics.peak_tps);
        info!("  Average Latency: {:.2}ms", metrics.average_latency_ms);
        info!("  Finality Time: {:.2}ms", metrics.finality_time_ms);
        info!("  Cross-shard Latency: {:.2}ms", metrics.cross_shard_latency_ms);
        info!("  Fractal Efficiency: {:.2}%", metrics.fractal_efficiency * 100.0);
        info!("  Memory Usage: {:.1}MB", metrics.memory_usage_mb);
        info!("  CPU Usage: {:.1}%", metrics.cpu_usage_percent);
    }
    
    /// Log error with context
    pub fn log_error<E: std::error::Error>(error: E, context: &str) {
        error!("{}: {}", context, error);
    }
    
    /// Log warning with context
    pub fn log_warning(message: &str, context: &str) {
        warn!("{}: {}", context, message);
    }
    
    /// Log info message
    pub fn log_info(message: &str, context: &str) {
        info!("{}: {}", context, message);
    }
    
    /// Log debug message
    pub fn log_debug(message: &str, context: &str) {
        debug!("{}: {}", context, message);
    }
}

/// Performance monitoring
pub mod monitoring {
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};
    use super::*;
    
    /// Performance monitor
    pub struct PerformanceMonitor {
        metrics_history: VecDeque<PerformanceMetrics>,
        window_size: usize,
        start_time: Instant,
    }
    
    impl PerformanceMonitor {
        /// Create new performance monitor
        pub fn new(window_size: usize) -> Self {
            PerformanceMonitor {
                metrics_history: VecDeque::with_capacity(window_size),
                window_size,
                start_time: Instant::now(),
            }
        }
        
        /// Record performance metrics
        pub fn record_metrics(&mut self, metrics: PerformanceMetrics) {
            if self.metrics_history.len() >= self.window_size {
                self.metrics_history.pop_front();
            }
            self.metrics_history.push_back(metrics);
        }
        
        /// Get average metrics over window
        pub fn get_average_metrics(&self) -> PerformanceMetrics {
            if self.metrics_history.is_empty() {
                return PerformanceMetrics::default();
            }
            
            let count = self.metrics_history.len();
            let mut avg_metrics = PerformanceMetrics::new();
            
            for metrics in &self.metrics_history {
                avg_metrics.current_tps += metrics.current_tps;
                avg_metrics.peak_tps += metrics.peak_tps;
                avg_metrics.average_latency_ms += metrics.average_latency_ms;
                avg_metrics.finality_time_ms += metrics.finality_time_ms;
                avg_metrics.cross_shard_latency_ms += metrics.cross_shard_latency_ms;
                avg_metrics.fractal_efficiency += metrics.fractal_efficiency;
                avg_metrics.memory_usage_mb += metrics.memory_usage_mb;
                avg_metrics.cpu_usage_percent += metrics.cpu_usage_percent;
            }
            
            avg_metrics.current_tps /= count as f64;
            avg_metrics.peak_tps /= count as f64;
            avg_metrics.average_latency_ms /= count as f64;
            avg_metrics.finality_time_ms /= count as f64;
            avg_metrics.cross_shard_latency_ms /= count as f64;
            avg_metrics.fractal_efficiency /= count as f64;
            avg_metrics.memory_usage_mb /= count as f64;
            avg_metrics.cpu_usage_percent /= count as f64;
            
            avg_metrics
        }
        
        /// Check if performance is degrading
        pub fn is_performance_degrading(&self) -> bool {
            if self.metrics_history.len() < 10 {
                return false;
            }
            
            let recent = &self.metrics_history[self.metrics_history.len()-10..];
            let recent_avg = recent.iter().map(|m| m.current_tps).sum::<f64>() / 10.0;
            
            let older = &self.metrics_history[0..10];
            let older_avg = older.iter().map(|m| m.current_tps).sum::<f64>() / 10.0;
            
            recent_avg < older_avg * 0.8 // 20% degradation
        }
        
        /// Get uptime
        pub fn get_uptime(&self) -> Duration {
            self.start_time.elapsed()
        }
    }
}

/// Configuration validation
pub mod validation {
    use super::*;
    
    /// Validate node configuration
    pub fn validate_config(config: &NodeConfig) -> Result<(), errors::FractalChainError> {
        // Validate network config
        if config.network.max_peers_per_shard == 0 {
            return Err(errors::FractalChainError::ConfigError(
                "max_peers_per_shard must be > 0".to_string()
            ));
        }
        
        if config.network.cross_shard_latency_target_ms == 0 {
            return Err(errors::FractalChainError::ConfigError(
                "cross_shard_latency_target_ms must be > 0".to_string()
            ));
        }
        
        // Validate consensus config
        if config.consensus.mining_threads == 0 && config.consensus.enable_mining {
            return Err(errors::FractalChainError::ConfigError(
                "mining_threads must be > 0 when mining is enabled".to_string()
            ));
        }
        
        if config.consensus.proposal_timeout_ms < config.consensus.vote_timeout_ms {
            return Err(errors::FractalChainError::ConfigError(
                "proposal_timeout_ms must be >= vote_timeout_ms".to_string()
            ));
        }
        
        // Validate state config
        if config.state.state_expiry_time_secs == 0 && config.state.enable_state_expiry {
            return Err(errors::FractalChainError::ConfigError(
                "state_expiry_time_secs must be > 0 when state expiry is enabled".to_string()
            ));
        }
        
        // Validate performance config
        if config.performance.target_tps == 0 {
            return Err(errors::FractalChainError::ConfigError(
                "target_tps must be > 0".to_string()
            ));
        }
        
        if config.performance.max_latency_ms == 0 {
            return Err(errors::FractalChainError::ConfigError(
                "max_latency_ms must be > 0".to_string()
            ));
        }
        
        if config.performance.performance_window_size == 0 {
            return Err(errors::FractalChainError::ConfigError(
                "performance_window_size must be > 0".to_string()
            ));
        }
        
        Ok(())
    }
    
    /// Validate performance metrics
    pub fn validate_performance_metrics(metrics: &PerformanceMetrics) -> Result<(), errors::FractalChainError> {
        if metrics.current_tps < 0.0 {
            return Err(errors::FractalChainError::PerformanceError(
                "current_tps cannot be negative".to_string()
            ));
        }
        
        if metrics.average_latency_ms < 0.0 {
            return Err(errors::FractalChainError::PerformanceError(
                "average_latency_ms cannot be negative".to_string()
            ));
        }
        
        if metrics.finality_time_ms < 0.0 {
            return Err(errors::FractalChainError::PerformanceError(
                "finality_time_ms cannot be negative".to_string()
            ));
        }
        
        if metrics.fractal_efficiency < 0.0 || metrics.fractal_efficiency > 1.0 {
            return Err(errors::FractalChainError::PerformanceError(
                "fractal_efficiency must be between 0.0 and 1.0".to_string()
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_system_info() {
        let info = SystemInfo::new();
        assert_eq!(info.chain_id, 859);
        assert_eq!(info.genesis_timestamp, GENESIS_TIMESTAMP);
        assert_eq!(info.fractal_depth, MAX_FRACTAL_DEPTH);
        assert_eq!(info.total_shards, SHARD_BASE);
    }
    
    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::new();
        metrics.current_tps = 9_000_000.0;
        metrics.finality_time_ms = 500.0;
        metrics.cross_shard_latency_ms = 50.0;
        metrics.fractal_efficiency = 0.95;
        
        assert!(metrics.targets_met());
    }
    
    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();
        assert_eq!(config.performance.target_tps, TARGET_TPS);
        assert_eq!(config.network.cross_shard_latency_target_ms, 100);
        assert!(config.state.enable_state_expiry);
    }
    
    #[test]
    fn test_performance_monitor() {
        let mut monitor = PerformanceMonitor::new(100);
        
        for i in 0..10 {
            let mut metrics = PerformanceMetrics::new();
            metrics.current_tps = (i + 1) as f64 * 1_000_000.0;
            monitor.record_metrics(metrics);
        }
        
        let avg_metrics = monitor.get_average_metrics();
        assert!(avg_metrics.current_tps > 0.0);
    }
    
    #[test]
    fn test_utils() {
        let data = b"test data";
        let hash = utils::hash(data);
        assert_eq!(hash.len(), 32);
        
        let hex_str = utils::to_hex(&hash);
        assert_eq!(hex_str.len(), 64);
        
        let bytes = utils::from_hex(&hex_str).unwrap();
        assert_eq!(bytes, hash);
    }
}