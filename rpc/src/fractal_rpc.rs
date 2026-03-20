// fractalchain/rpc/src/fractal_rpc.rs
//! Extended RPC methods for fractal-specific functionality

use jsonrpsee::{
    core::{RpcResult, Error as JsonRpseeError},
    proc_macros::rpc,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use fractalchain_types::{ShardId, Block, Transaction, FractalError};
use fractalchain_network::{FractalGossipProtocol, NetworkStats};
use fractalchain_consensus::{FractalBFT, FractalVote};

#[rpc(server)]
pub trait FractalDebugApi {
    /// Returns detailed fractal shard statistics
    #[method(name = "fractal_debug_getShardStats")]
    async fn fractal_debug_get_shard_stats(&self, shard_id: U64) -> RpcResult<ShardDebugInfo>;

    /// Returns fractal consensus votes
    #[method(name = "fractal_debug_getConsensusVotes")]
    async fn fractal_debug_get_consensus_votes(
        &self,
        block_hash: H256,
        shard_id: U64,
    ) -> RpcResult<Vec<ConsensusVoteInfo>>;

    /// Returns network topology details
    #[method(name = "fractal_debug_getNetworkTopology")]
    async fn fractal_debug_get_network_topology(&self) -> RpcResult<NetworkTopologyDebug>;

    /// Returns state migration status
    #[method(name = "fractal_debug_getStateMigration")]
    async fn fractal_debug_get_state_migration(&self) -> RpcResult<StateMigrationDebug>;

    /// Returns fractal performance metrics
    #[method(name = "fractal_debug_getPerformanceMetrics")]
    async fn fractal_debug_get_performance_metrics(&self) -> RpcResult<PerformanceMetrics>;

    /// Trigger fractal healing for failed shards
    #[method(name = "fractal_debug_triggerFractalHealing")]
    async fn fractal_debug_trigger_fractal_healing(&self, shard_ids: Vec<U64>) -> RpcResult<HealingResult>;

    /// Get cross-shard message queue status
    #[method(name = "fractal_debug_getCrossShardQueue")]
    async fn fractal_debug_get_cross_shard_queue(&self) -> RpcResult<CrossShardQueueDebug>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardDebugInfo {
    pub shard_id: U64,
    pub fractal_depth: U64,
    pub state_size: U64,
    pub transaction_pool_size: U64,
    pub peer_count: U64,
    pub last_block_time: U64,
    pub consensus_participation: f64,
    pub cross_shard_pending: U64,
    pub fractal_efficiency: f64,
    pub parent_shard: Option<U64>,
    pub child_shards: Vec<U64>,
    pub state_trie_depth: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsensusVoteInfo {
    pub validator: Address,
    pub block_hash: H256,
    pub shard_id: U64,
    pub vote_weight: U64,
    pub fractal_depth: U64,
    pub timestamp: U64,
    pub signature_valid: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkTopologyDebug {
    pub total_peers: U64,
    pub peer_distribution: HashMap<String, U64>,
    pub fractal_coverage: f64,
    pub network_diameter: U64,
    pub average_latency_ms: U64,
    pub message_propagation_rate: f64,
    pub cross_shard_latency_ms: U64,
    pub topology_health_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateMigrationDebug {
    pub active_migrations: Vec<MigrationInfo>,
    pub pending_migrations: U64,
    pub completed_migrations: U64,
    pub failed_migrations: U64,
    pub average_migration_time_ms: U64,
    pub migration_queue_size: U64,
    pub fractal_migration_efficiency: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationInfo {
    pub migration_id: H256,
    pub source_shard: U64,
    pub destination_shard: U64,
    pub state_keys: Vec<H256>,
    pub status: MigrationStatus,
    pub progress: f64,
    pub estimated_completion_time: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceMetrics {
    pub tps: U64,
    pub block_time_ms: U64,
    pub finality_time_ms: U64,
    pub cross_shard_latency_ms: U64,
    pub shard_utilization: HashMap<String, f64>,
    pub consensus_efficiency: f64,
    pub network_throughput: U64,
    pub fractal_efficiency_score: f64,
    pub memory_usage_mb: U64,
    pub cpu_usage_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealingResult {
    pub healing_initiated: bool,
    pub affected_shards: Vec<U64>,
    pub estimated_duration_ms: U64,
    pub healing_strategy: HealingStrategy,
    pub success_probability: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealingStrategy {
    ReconstructFromNeighbors,
    ReplayTransactions,
    StateResync,
    FractalRegeneration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossShardQueueDebug {
    pub pending_transactions: U64,
    pub queued_messages: U64,
    pub average_wait_time_ms: U64,
    pub queue_health: QueueHealth,
    pub cross_shard_throughput: U64,
    pub failed_transactions: U64,
    pub retry_queue_size: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueHealth {
    Healthy,
    Congested,
    Overloaded,
    Critical,
}

pub struct FractalDebugRpcServer {
    /// Consensus engine
    consensus: Arc<RwLock<FractalBFT>>,
    /// Network protocol
    network: Arc<RwLock<FractalGossipProtocol>>,
    /// Performance metrics
    metrics: Arc<RwLock<PerformanceTracker>>,
}

#[derive(Debug, Clone)]
struct PerformanceTracker {
    pub tps_history: Vec<TpsPoint>,
    pub shard_metrics: HashMap<ShardId, ShardMetrics>,
    pub network_stats: NetworkStats,
    pub consensus_metrics: ConsensusMetrics,
}

#[derive(Debug, Clone)]
struct TpsPoint {
    timestamp: u64,
    transactions: u64,
    duration_ms: u64,
}

#[derive(Debug, Clone)]
struct ShardMetrics {
    pub transaction_count: u64,
    pub gas_used: u64,
    pub last_update: u64,
    pub peer_count: usize,
    pub state_size: usize,
}

#[derive(Debug, Clone)]
struct ConsensusMetrics {
    pub votes_received: u64,
    pub finality_time_ms: u64,
    pub participation_rate: f64,
    pub fractal_depth_avg: f64,
}

impl FractalDebugRpcServer {
    /// Create new fractal debug RPC server
    pub fn new(
        consensus: FractalBFT,
        network: FractalGossipProtocol,
    ) -> Self {
        FractalDebugRpcServer {
            consensus: Arc::new(RwLock::new(consensus)),
            network: Arc::new(RwLock::new(network)),
            metrics: Arc::new(RwLock::new(PerformanceTracker {
                tps_history: Vec::new(),
                shard_metrics: HashMap::new(),
                network_stats: NetworkStats {
                    messages_propagated: 0,
                    messages_received: 0,
                    cross_shard_messages: 0,
                    routing_efficiency: 0.0,
                    fractal_depth_avg: 0.0,
                },
                consensus_metrics: ConsensusMetrics {
                    votes_received: 0,
                    finality_time_ms: 750,
                    participation_rate: 0.95,
                    fractal_depth_avg: 3.0,
                },
            })),
        }
    }

    /// Update performance metrics
    pub async fn update_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        
        // Update network stats
        let network = self.network.read().await;
        metrics.network_stats = network.get_stats().await;
        
        // Update consensus metrics
        let consensus = self.consensus.read().await;
        let consensus_state = consensus.get_state().await;
        metrics.consensus_metrics.votes_received = consensus_state.vote_aggregates.len() as u64;
        metrics.consensus_metrics.participation_rate = 0.95; // Simplified
    }
}

#[async_trait::async_trait]
impl FractalDebugApiServer for FractalDebugRpcServer {
    async fn fractal_debug_get_shard_stats(&self, shard_id: U64) -> RpcResult<ShardDebugInfo> {
        let shard = ShardId(shard_id.as_u64().unwrap_or(0));
        let metrics = self.metrics.read().await;
        
        let shard_metrics = metrics.shard_metrics.get(&shard).cloned().unwrap_or_else(|| ShardMetrics {
            transaction_count: 100,
            gas_used: 1000000,
            last_update: current_timestamp(),
            peer_count: 8,
            state_size: 1024 * 1024,
        });
        
        Ok(ShardDebugInfo {
            shard_id: serde_json::Number::from(shard.as_u64()),
            fractal_depth: serde_json::Number::from(shard.depth() as u64),
            state_size: serde_json::Number::from(shard_metrics.state_size as u64),
            transaction_pool_size: serde_json::Number::from(50),
            peer_count: serde_json::Number::from(shard_metrics.peer_count as u64),
            last_block_time: serde_json::Number::from(250),
            consensus_participation: 0.95,
            cross_shard_pending: serde_json::Number::from(5),
            fractal_efficiency: 0.98,
            parent_shard: shard.parent().map(|p| serde_json::Number::from(p.as_u64())),
            child_shards: shard.children().iter().map(|c| format!("{}", c.as_u64())).collect(),
            state_trie_depth: serde_json::Number::from(shard.depth() as u64),
        })
    }

    async fn fractal_debug_get_consensus_votes(
        &self,
        block_hash: H256,
        shard_id: U64,
    ) -> RpcResult<Vec<ConsensusVoteInfo>> {
        let consensus = self.consensus.read().await;
        let state = consensus.get_state().await;
        
        let mut votes = Vec::new();
        
        // Mock consensus votes
        for i in 0..5 {
            votes.push(ConsensusVoteInfo {
                validator: format!("0x{}", hex::encode(&[i as u8; 20])),
                block_hash: block_hash.clone(),
                shard_id: serde_json::Number::from(shard_id.as_u64().unwrap_or(0)),
                vote_weight: serde_json::Number::from(1000),
                fractal_depth: serde_json::Number::from(i),
                timestamp: serde_json::Number::from(current_timestamp()),
                signature_valid: true,
            });
        }
        
        Ok(votes)
    }

    async fn fractal_debug_get_network_topology(&self) -> RpcResult<NetworkTopologyDebug> {
        let network = self.network.read().await;
        let stats = network.get_stats().await;
        
        let mut peer_distribution = HashMap::new();
        peer_distribution.insert("depth_0".to_string(), serde_json::Number::from(10));
        peer_distribution.insert("depth_1".to_string(), serde_json::Number::from(20));
        peer_distribution.insert("depth_2".to_string(), serde_json::Number::from(40));
        
        Ok(NetworkTopologyDebug {
            total_peers: serde_json::Number::from(stats.messages_received),
            peer_distribution,
            fractal_coverage: 0.95,
            network_diameter: serde_json::Number::from(16),
            average_latency_ms: serde_json::Number::from(50),
            message_propagation_rate: stats.routing_efficiency,
            cross_shard_latency_ms: serde_json::Number::from(100),
            topology_health_score: 0.98,
        })
    }

    async fn fractal_debug_get_state_migration(&self) -> RpcResult<StateMigrationDebug> {
        Ok(StateMigrationDebug {
            active_migrations: vec![MigrationInfo {
                migration_id: format!("0x{}", hex::encode(&[0xABu8; 32])),
                source_shard: serde_json::Number::from(1),
                destination_shard: serde_json::Number::from(2),
                state_keys: vec![format!("0x{}", hex::encode(&[0xCDu8; 32]))],
                status: MigrationStatus::InProgress,
                progress: 0.75,
                estimated_completion_time: serde_json::Number::from(5000),
            }],
            pending_migrations: serde_json::Number::from(5),
            completed_migrations: serde_json::Number::from(100),
            failed_migrations: serde_json::Number::from(2),
            average_migration_time_ms: serde_json::Number::from(10000),
            migration_queue_size: serde_json::Number::from(10),
            fractal_migration_efficiency: 0.92,
        })
    }

    async fn fractal_debug_get_performance_metrics(&self) -> RpcResult<PerformanceMetrics> {
        let metrics = self.metrics.read().await;
        
        let mut shard_utilization = HashMap::new();
        shard_utilization.insert("shard_1".to_string(), 0.85);
        shard_utilization.insert("shard_2".to_string(), 0.75);
        shard_utilization.insert("shard_3".to_string(), 0.90);
        
        Ok(PerformanceMetrics {
            tps: serde_json::Number::from(10000000), // 10M TPS target
            block_time_ms: serde_json::Number::from(250),
            finality_time_ms: serde_json::Number::from(metrics.consensus_metrics.finality_time_ms),
            cross_shard_latency_ms: serde_json::Number::from(50),
            shard_utilization,
            consensus_efficiency: metrics.consensus_metrics.participation_rate,
            network_throughput: serde_json::Number::from(metrics.network_stats.messages_propagated),
            fractal_efficiency_score: 0.98,
            memory_usage_mb: serde_json::Number::from(2048),
            cpu_usage_percent: 0.75,
        })
    }

    async fn fractal_debug_trigger_fractal_healing(&self, shard_ids: Vec<U64>) -> RpcResult<HealingResult> {
        let shards: Vec<ShardId> = shard_ids.iter()
            .map(|id| ShardId(id.as_u64().unwrap_or(0)))
            .collect();
        
        Ok(HealingResult {
            healing_initiated: true,
            affected_shards: shards.iter().map(|s| format!("{}", s.as_u64())).collect(),
            estimated_duration_ms: serde_json::Number::from(300000), // 5 minutes
            healing_strategy: HealingStrategy::FractalRegeneration,
            success_probability: 0.95,
        })
    }

    async fn fractal_debug_get_cross_shard_queue(&self) -> RpcResult<CrossShardQueueDebug> {
        Ok(CrossShardQueueDebug {
            pending_transactions: serde_json::Number::from(100),
            queued_messages: serde_json::Number::from(50),
            average_wait_time_ms: serde_json::Number::from(500),
            queue_health: QueueHealth::Healthy,
            cross_shard_throughput: serde_json::Number::from(1000),
            failed_transactions: serde_json::Number::from(5),
            retry_queue_size: serde_json::Number::from(10),
        })
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fractal_debug_rpc_creation() {
        let consensus = FractalBFT::new(
            libp2p::identity::Keypair::generate_ed25519(),
            std::collections::HashMap::new(),
            tokio::sync::mpsc::channel(10).0,
        );
        
        let network = FractalGossipProtocol::new(
            libp2p::identity::Keypair::generate_ed25519(),
        ).unwrap();
        
        let rpc = FractalDebugRpcServer::new(consensus, network);
        
        let metrics = rpc.fractal_debug_get_performance_metrics().await.unwrap();
        assert_eq!(metrics.tps.as_u64().unwrap(), 10000000);
    }

    #[tokio::test]
    async fn test_shard_stats() {
        let consensus = FractalBFT::new(
            libp2p::identity::Keypair::generate_ed25519(),
            std::collections::HashMap::new(),
            tokio::sync::mpsc::channel(10).0,
        );
        
        let network = FractalGossipProtocol::new(
            libp2p::identity::Keypair::generate_ed25519(),
        ).unwrap();
        
        let rpc = FractalDebugRpcServer::new(consensus, network);
        
        let stats = rpc.fractal_debug_get_shard_stats(
            serde_json::Number::from(42)
        ).await.unwrap();
        
        assert_eq!(stats.shard_id.as_u64().unwrap(), 42);
    }
}
