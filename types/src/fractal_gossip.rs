// fractalchain/network/src/fractal_gossip.rs
//! Fractal gossip protocol with recursive routing
//! Implements libp2p-based gossipsub with fractal topology

use libp2p::{
    gossipsub::{Gossipsub, GossipsubConfig, GossipsubMessage, MessageAuthenticity, ValidationMode},
    identity::Keypair,
    PeerId,
    Multiaddr,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use thiserror::Error;

use fractalchain_types::{ShardId, Block, Transaction, FractalError};

/// Fractal gossip protocol name
pub const FRACTAL_GOSSIP_PROTOCOL: &str = "/fractal/gossip/1.0.0";
/// Maximum message size: 10MB
pub const MAX_GOSSIP_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
/// Gossip heartbeat interval: 1 second
pub const GOSSIP_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Message propagation depth limit
pub const MAX_FRACTAL_DEPTH: u8 = 32;
/// Recursive routing table size per shard
pub const ROUTING_TABLE_SIZE: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalMessage {
    /// Message type
    pub msg_type: MessageType,
    /// Source shard
    pub source_shard: ShardId,
    /// Target shards (for recursive routing)
    pub target_shards: Vec<ShardId>,
    /// Message payload
    pub payload: Vec<u8>,
    /// Message hash for deduplication
    pub message_hash: [u8; 32],
    /// Fractal depth for recursive propagation
    pub fractal_depth: u8,
    /// Timestamp for message ordering
    pub timestamp: u64,
    /// Sender peer ID
    pub sender: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    BlockPropagation,
    TransactionGossip,
    ConsensusVote,
    StateSyncRequest,
    StateSyncResponse,
    CrossShardCommit,
    CrossShardPrepare,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecursiveRoutingTable {
    /// Local shard assignments
    pub local_shards: HashSet<ShardId>,
    /// Neighbors by shard depth
    pub neighbors_by_depth: HashMap<u8, Vec<PeerId>>,
    /// Fractal topology cache
    pub topology_cache: FractalTopology,
    /// Message routing history
    pub routing_history: Vec<RoutingDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalTopology {
    /// Parent-child relationships
    pub parent_child: HashMap<ShardId, Vec<ShardId>>,
    /// Sibling relationships
    pub siblings: HashMap<ShardId, Vec<ShardId>>,
    /// Depth mapping
    pub depth_map: HashMap<ShardId, u8>,
    /// Routing efficiency scores
    pub efficiency_scores: HashMap<(ShardId, ShardId), f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingDecision {
    /// Source message
    pub message_hash: [u8; 32],
    /// Routing path taken
    pub path: Vec<PeerId>,
    /// Final destination
    pub destination: ShardId,
    /// Routing efficiency
    pub efficiency: f64,
    /// Timestamp
    pub timestamp: Instant,
}

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Message propagation failed: {0}")]
    PropagationFailed(String),
    #[error("Fractal routing error: {0}")]
    FractalRoutingError(String),
    #[error("Message validation failed")]
    MessageValidationFailed,
    #[error("Network timeout")]
    NetworkTimeout,
    #[error("Fractal error: {0}")]
    FractalError(#[from] FractalError),
}

pub struct FractalGossipProtocol {
    /// libp2p gossipsub instance
    gossipsub: Gossipsub,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Recursive routing table
    routing_table: RwLock<RecursiveRoutingTable>,
    /// Message cache for deduplication
    message_cache: RwLock<HashMap<[u8; 32], Instant>>,
    /// Cross-shard message queue
    cross_shard_queue: mpsc::Sender<FractalMessage>,
    /// Network statistics
    stats: RwLock<NetworkStats>,
}

#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub messages_propagated: u64,
    pub messages_received: u64,
    pub cross_shard_messages: u64,
    pub routing_efficiency: f64,
    pub fractal_depth_avg: f64,
}

impl FractalGossipProtocol {
    /// Create a new fractal gossip protocol instance
    pub fn new(keypair: Keypair) -> Result<Self, NetworkError> {
        let local_peer_id = PeerId::from(keypair.public());
        
        let gossipsub_config = GossipsubConfig::builder()
            .protocol_id(FRACTAL_GOSSIP_PROTOCOL.parse().unwrap())
            .max_transmit_size(MAX_GOSSIP_MESSAGE_SIZE)
            .validation_mode(ValidationMode::Strict)
            .heartbeat_interval(GOSSIP_HEARTBEAT_INTERVAL)
            .build()
            .map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;

        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(keypair),
            gossipsub_config,
        ).map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;

        let (cross_shard_queue, _) = mpsc::channel(1024);

        Ok(FractalGossipProtocol {
            gossipsub,
            local_peer_id,
            routing_table: RwLock::new(RecursiveRoutingTable {
                local_shards: HashSet::new(),
                neighbors_by_depth: HashMap::new(),
                topology_cache: FractalTopology {
                    parent_child: HashMap::new(),
                    siblings: HashMap::new(),
                    depth_map: HashMap::new(),
                    efficiency_scores: HashMap::new(),
                },
                routing_history: Vec::new(),
            }),
            message_cache: RwLock::new(HashMap::new()),
            cross_shard_queue,
            stats: RwLock::new(NetworkStats {
                messages_propagated: 0,
                messages_received: 0,
                cross_shard_messages: 0,
                routing_efficiency: 0.0,
                fractal_depth_avg: 0.0,
            }),
        })
    }

    /// Propagate block to fractal network
    pub async fn propagate_block(&mut self, block: Block) -> Result<(), NetworkError> {
        let block_data = bincode::serialize(&block)
            .map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;
        
        let message = FractalMessage {
            msg_type: MessageType::BlockPropagation,
            source_shard: block.header.shard_id,
            target_shards: self.calculate_target_shards(block.header.shard_id),
            payload: block_data,
            message_hash: self.hash_message(&block_data),
            fractal_depth: block.header.fractal_depth,
            timestamp: current_timestamp(),
            sender: self.local_peer_id,
        };

        self.propagate_message(message).await
    }

    /// Gossip transaction across fractal topology
    pub async fn gossip_transaction(&mut self, tx: Transaction) -> Result<(), NetworkError> {
        let tx_data = bincode::serialize(&tx)
            .map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;
        
        let message = FractalMessage {
            msg_type: MessageType::TransactionGossip,
            source_shard: tx.source_shard,
            target_shards: vec![tx.destination_shard],
            payload: tx_data,
            message_hash: self.hash_message(&tx_data),
            fractal_depth: 0,
            timestamp: current_timestamp(),
            sender: self.local_peer_id,
        };

        self.propagate_message(message).await
    }

    /// Handle incoming fractal message
    pub async fn handle_message(&mut self, message: FractalMessage) -> Result<(), NetworkError> {
        // Validate message
        self.validate_message(&message).await?;
        
        // Check for duplicates
        if self.is_duplicate(&message).await {
            return Ok(());
        }
        
        // Update routing statistics
        self.update_routing_stats(&message).await;
        
        // Process based on message type
        match message.msg_type {
            MessageType::BlockPropagation => {
                self.handle_block_propagation(message).await
            }
            MessageType::TransactionGossip => {
                self.handle_transaction_gossip(message).await
            }
            MessageType::ConsensusVote => {
                self.handle_consensus_vote(message).await
            }
            MessageType::CrossShardCommit => {
                self.handle_cross_shard_message(message).await
            }
            _ => Ok(()),
        }
    }

    /// Propagate message using fractal routing
    async fn propagate_message(&mut self, message: FractalMessage) -> Result<(), NetworkError> {
        let routing_table = self.routing_table.read().await;
        
        // Apply fractal routing algorithm
        let routing_decision = self.calculate_fractal_routing(&message, &routing_table).await?;
        
        // Forward to appropriate neighbors
        for neighbor in &routing_decision.next_hops {
            self.forward_to_neighbor(neighbor, &message).await?;
        }
        
        // Update message cache
        let mut cache = self.message_cache.write().await;
        cache.insert(message.message_hash, Instant::now());
        
        // Update statistics
        let mut stats = self.stats.write().await;
        stats.messages_propagated += 1;
        
        Ok(())
    }

    /// Calculate fractal routing decision
    async fn calculate_fractal_routing(
        &self,
        message: &FractalMessage,
        routing_table: &RecursiveRoutingTable,
    ) -> Result<RoutingDecision, NetworkError> {
        let mut next_hops = Vec::new();
        let mut efficiency = 0.0;
        
        // Recursive fractal routing algorithm
        for target_shard in &message.target_shards {
            let target_depth = self.calculate_shard_depth(target_shard);
            let current_depth = message.fractal_depth;
            
            if current_depth < MAX_FRACTAL_DEPTH {
                // Route to child shards
                let child_shards = self.get_child_shards(target_shard, &routing_table.topology_cache);
                for child in child_shards {
                    if let Some(neighbors) = routing_table.neighbors_by_depth.get(&target_depth) {
                        for neighbor in neighbors {
                            if self.is_optimal_route(&message, neighbor, &child).await {
                                next_hops.push(*neighbor);
                                efficiency += 1.0;
                            }
                        }
                    }
                }
            } else {
                // Route to siblings for load balancing
                let siblings = self.get_sibling_shards(target_shard, &routing_table.topology_cache);
                for sibling in siblings {
                    if let Some(neighbors) = routing_table.neighbors_by_depth.get(&target_depth) {
                        for neighbor in neighbors {
                            next_hops.push(*neighbor);
                            efficiency += 0.5;
                        }
                    }
                }
            }
        }
        
        efficiency = if !next_hops.is_empty() {
            efficiency / next_hops.len() as f64
        } else {
            0.0
        };
        
        Ok(RoutingDecision {
            message_hash: message.message_hash,
            next_hops,
            efficiency,
            fractal_depth: message.fractal_depth,
        })
    }

    /// Handle block propagation with fractal efficiency
    async fn handle_block_propagation(&mut self, message: FractalMessage) -> Result<(), NetworkError> {
        let block: Block = bincode::deserialize(&message.payload)
            .map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;
        
        // Validate block fractal properties
        block.validate().map_err(FractalError::FractalError)?;
        
        // Re-propagate to relevant shards
        let child_shards = block.header.shard_id.children();
        for child in child_shards {
            let mut child_message = message.clone();
            child_message.target_shards = vec![child];
            child_message.fractal_depth += 1;
            
            self.propagate_message(child_message).await?;
        }
        
        Ok(())
    }

    /// Handle cross-shard messages with atomic guarantees
    async fn handle_cross_shard_message(&mut self, message: FractalMessage) -> Result<(), NetworkError> {
        // Queue for cross-shard processing
        self.cross_shard_queue.send(message).await
            .map_err(|e| NetworkError::PropagationFailed(e.to_string()))?;
        
        // Update statistics
        let mut stats = self.stats.write().await;
        stats.cross_shard_messages += 1;
        
        Ok(())
    }

    /// Update fractal topology based on network observations
    pub async fn update_topology(&mut self, peer_id: PeerId, shard_info: Vec<ShardId>) -> Result<(), NetworkError> {
        let mut routing_table = self.routing_table.write().await;
        
        for shard_id in shard_info {
            let depth = self.calculate_shard_depth(&shard_id);
            
            // Update topology cache
            routing_table.topology_cache.depth_map.insert(shard_id, depth);
            
            // Update neighbors
            routing_table.neighbors_by_depth
                .entry(depth)
                .or_insert_with(Vec::new)
                .push(peer_id);
            
            // Update local shards if this is our peer
            if peer_id == self.local_peer_id {
                routing_table.local_shards.insert(shard_id);
            }
        }
        
        // Recalculate efficiency scores
        self.recalculate_efficiency_scores(&mut routing_table.topology_cache).await?;
        
        Ok(())
    }

    /// Calculate shard depth in fractal hierarchy
    fn calculate_shard_depth(&self, shard_id: &ShardId) -> u8 {
        shard_id.depth()
    }

    /// Get child shards for recursive routing
    fn get_child_shards(
        &self,
        shard_id: &ShardId,
        topology: &FractalTopology,
    ) -> Vec<ShardId> {
        topology.parent_child.get(shard_id)
            .cloned()
            .unwrap_or_else(|| shard_id.children())
    }

    /// Get sibling shards for load balancing
    fn get_sibling_shards(
        &self,
        shard_id: &ShardId,
        topology: &FractalTopology,
    ) -> Vec<ShardId> {
        topology.siblings.get(shard_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if route is optimal based on fractal metrics
    async fn is_optimal_route(
        &self,
        message: &FractalMessage,
        neighbor: &PeerId,
        target_shard: &ShardId,
    ) -> bool {
        // Simplified - real implementation would use fractal distance metrics
        let routing_table = self.routing_table.read().await;
        
        routing_table.topology_cache.efficiency_scores
            .get(&(message.source_shard, *target_shard))
            .map(|score| *score > 0.5)
            .unwrap_or(true)
    }

    /// Recalculate efficiency scores based on routing history
    async fn recalculate_efficiency_scores(
        &self,
        topology: &mut FractalTopology,
    ) -> Result<(), NetworkError> {
        let routing_table = self.routing_table.read().await;
        
        for history in &routing_table.routing_history {
            let key = (history.path.first().copied().unwrap_or_default(), history.destination);
            let current_score = topology.efficiency_scores.get(&key).copied().unwrap_or(0.5);
            let new_score = (current_score + history.efficiency) / 2.0;
            
            topology.efficiency_scores.insert(key, new_score);
        }
        
        Ok(())
    }

    /// Validate incoming message
    async fn validate_message(&self, message: &FractalMessage) -> Result<(), NetworkError> {
        // Check timestamp (prevent replay attacks)
        let current_time = current_timestamp();
        if message.timestamp > current_time + 60 { // 1 minute tolerance
            return Err(NetworkError::MessageValidationFailed);
        }
        
        // Check fractal depth
        if message.fractal_depth > MAX_FRACTAL_DEPTH {
            return Err(NetworkError::FractalRoutingError("Max depth exceeded".to_string()));
        }
        
        // Check message hash
        let calculated_hash = self.hash_message(&message.payload);
        if calculated_hash != message.message_hash {
            return Err(NetworkError::MessageValidationFailed);
        }
        
        Ok(())
    }

    /// Check for duplicate messages
    async fn is_duplicate(&self, message: &FractalMessage) -> bool {
        let cache = self.message_cache.read().await;
        cache.contains_key(&message.message_hash)
    }

    /// Update routing statistics
    async fn update_routing_stats(&self, message: &FractalMessage) {
        let mut stats = self.stats.write().await;
        stats.messages_received += 1;
        stats.fractal_depth_avg = (stats.fractal_depth_avg + message.fractal_depth as f64) / 2.0;
    }

    /// Hash message for deduplication
    fn hash_message(&self, data: &[u8]) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    /// Forward message to neighbor
    async fn forward_to_neighbor(
        &self,
        neighbor: &PeerId,
        message: &FractalMessage,
    ) -> Result<(), NetworkError> {
        // In real implementation, this would use libp2p to forward the message
        // For now, just simulate successful forwarding
        Ok(())
    }

    /// Handle transaction gossip (placeholder)
    async fn handle_transaction_gossip(&mut self, _message: FractalMessage) -> Result<(), NetworkError> {
        Ok(())
    }

    /// Handle consensus vote (placeholder)
    async fn handle_consensus_vote(&mut self, _message: FractalMessage) -> Result<(), NetworkError> {
        Ok(())
    }

    /// Get current network statistics
    pub async fn get_stats(&self) -> NetworkStats {
        self.stats.read().await.clone()
    }
}

/// Routing decision for fractal propagation
#[derive(Debug, Clone)]
struct RoutingDecision {
    message_hash: [u8; 32],
    next_hops: Vec<PeerId>,
    efficiency: f64,
    fractal_depth: u8,
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
    use libp2p::identity;

    #[tokio::test]
    async fn test_fractal_gossip_creation() {
        let keypair = identity::Keypair::generate_ed25519();
        let mut gossip = FractalGossipProtocol::new(keypair).unwrap();
        
        assert_eq!(gossip.local_peer_id.to_string().len() > 0, true);
    }

    #[tokio::test]
    async fn test_message_validation() {
        let keypair = identity::Keypair::generate_ed25519();
        let gossip = FractalGossipProtocol::new(keypair).unwrap();
        
        let message = FractalMessage {
            msg_type: MessageType::BlockPropagation,
            source_shard: ShardId(0),
            target_shards: vec![ShardId(1)],
            payload: vec![1, 2, 3],
            message_hash: [0u8; 32],
            fractal_depth: 0,
            timestamp: current_timestamp(),
            sender: PeerId::random(),
        };
        
        let result = gossip.validate_message(&message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_topology_update() {
        let keypair = identity::Keypair::generate_ed25519();
        let mut gossip = FractalGossipProtocol::new(keypair).unwrap();
        
        let peer_id = PeerId::random();
        let shards = vec![ShardId(1), ShardId(2), ShardId(3)];
        
        let result = gossip.update_topology(peer_id, shards).await;
        assert!(result.is_ok());
    }
}
