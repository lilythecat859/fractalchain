// fractalchain/network/src/peer_discovery.rs
//! Fractal peer discovery with recursive topology building
//! Implements Kademlia-style discovery with fractal enhancements

use libp2p::{
    kad::{Kademlia, KademliaConfig, KademliaEvent, QueryId, Record},
    mdns::{Mdns, MdnsEvent},
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
    Multiaddr,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

use fractalchain_types::ShardId;

/// Kademlia protocol name for fractal network
pub const FRACTAL_KAD_PROTOCOL: &[u8] = b"/fractal/kad/1.0.0";
/// Maximum peers per shard for optimal performance
pub const MAX_PEERS_PER_SHARD: usize = 32;
/// Peer discovery interval: 30 seconds
pub const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
/// Peer quality score threshold
pub const PEER_QUALITY_THRESHOLD: f64 = 0.7;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalPeerInfo {
    /// Peer ID
    pub peer_id: PeerId,
    /// Peer multiaddresses
    pub addresses: Vec<Multiaddr>,
    /// Shards this peer is responsible for
    pub shard_responsibility: Vec<ShardId>,
    /// Peer quality score (0-1)
    pub quality_score: f64,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Fractal depth of peer's position
    pub fractal_depth: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalRoutingTable {
    /// Peers organized by fractal depth
    pub peers_by_depth: HashMap<u8, Vec<FractalPeerInfo>>,
    /// Shard to peer mapping
    pub shard_to_peers: HashMap<ShardId, Vec<PeerId>>,
    /// Peer quality tracking
    pub peer_quality: HashMap<PeerId, PeerQuality>,
    /// Fractal topology state
    pub topology_state: TopologyState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerQuality {
    pub latency_ms: u64,
    pub reliability_score: f64,
    pub bandwidth_score: f64,
    pub last_updated: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyState {
    /// Current fractal depth coverage
    pub depth_coverage: HashMap<u8, usize>,
    /// Network diameter estimation
    pub network_diameter: u8,
    /// Peer distribution quality
    pub distribution_quality: f64,
}

#[derive(NetworkBehaviour)]
pub struct FractalDiscovery {
    /// Kademlia DHT for peer discovery
    kademlia: Kademlia,
    /// mDNS for local peer discovery
    mdns: Mdns,
    /// Fractal routing table
    routing_table: RwLock<FractalRoutingTable>,
}

impl FractalDiscovery {
    /// Create new fractal discovery service
    pub fn new(local_peer_id: PeerId) -> Result<Self, Box<dyn std::error::Error>> {
        let mut kademlia_config = KademliaConfig::default();
        kademlia_config.set_protocol_name(FRACTAL_KAD_PROTOCOL);
        
        let kademlia = Kademlia::new(local_peer_id, kademlia_config);
        let mdns = Mdns::new(Default::default())?;
        
        let routing_table = RwLock::new(FractalRoutingTable {
            peers_by_depth: HashMap::new(),
            shard_to_peers: HashMap::new(),
            peer_quality: HashMap::new(),
            topology_state: TopologyState {
                depth_coverage: HashMap::new(),
                network_diameter: 0,
                distribution_quality: 0.0,
            },
        });

        Ok(FractalDiscovery {
            kademlia,
            mdns,
            routing_table,
        })
    }

    /// Discover peers for specific shard
    pub async fn discover_shard_peers(&mut self, shard_id: ShardId) -> Vec<FractalPeerInfo> {
        let mut routing_table = self.routing_table.write().await;
        
        // Check cache first
        if let Some(peers) = routing_table.shard_to_peers.get(&shard_id) {
            let mut result = Vec::new();
            for peer_id in peers {
                if let Some(peer_info) = self.find_peer_info(&routing_table, peer_id) {
                    result.push(peer_info);
                }
            }
            return result;
        }
        
        // Perform discovery
        self.perform_shard_discovery(&mut routing_table, shard_id).await
    }

    /// Perform targeted shard discovery
    async fn perform_shard_discovery(
        &self,
        routing_table: &mut FractalRoutingTable,
        shard_id: ShardId,
    ) -> Vec<FractalPeerInfo> {
        let shard_depth = shard_id.depth();
        let mut discovered_peers = Vec::new();
        
        // Query Kademlia for peers at specific depth
        let target_key = self.generate_shard_key(&shard_id);
        let query_id = self.kademlia.get_closest_peers(target_key);
        
        // Wait for query results (simplified)
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // Filter peers by quality and shard responsibility
        for (_, peer_info) in &routing_table.peers_by_depth {
            for peer in peer_info {
                if peer.shard_responsibility.contains(&shard_id) && 
                   peer.quality_score >= PEER_QUALITY_THRESHOLD {
                    discovered_peers.push(peer.clone());
                }
            }
        }
        
        // Limit to maximum peers per shard
        discovered_peers.truncate(MAX_PEERS_PER_SHARD);
        discovered_peers
    }

    /// Generate Kademlia key for shard targeting
    fn generate_shard_key(&self, shard_id: &ShardId) -> Vec<u8> {
        let mut key = vec![0u8; 32];
        key[0..8].copy_from_slice(&shard_id.as_u64().to_le_bytes());
        key[8..16].copy_from_slice(b"fractal_shard");
        key
    }

    /// Find peer info in routing table
    fn find_peer_info(
        &self,
        routing_table: &FractalRoutingTable,
        peer_id: &PeerId,
    ) -> Option<FractalPeerInfo> {
        for peers in routing_table.peers_by_depth.values() {
            for peer in peers {
                if peer.peer_id == *peer_id {
                    return Some(peer.clone());
                }
            }
        }
        None
    }

    /// Update peer quality metrics
    pub async fn update_peer_quality(
        &mut self,
        peer_id: PeerId,
        latency_ms: u64,
        reliability: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut routing_table = self.routing_table.write().await;
        
        let quality = PeerQuality {
            latency_ms,
            reliability_score: reliability,
            bandwidth_score: 1.0, // Simplified
            last_updated: Instant::now(),
        };
        
        routing_table.peer_quality.insert(peer_id, quality);
        
        // Recalculate fractal topology
        self.recalculate_topology(&mut routing_table).await?;
        
        Ok(())
    }

    /// Recalculate fractal topology based on peer distribution
    async fn recalculate_topology(
        &self,
        routing_table: &mut FractalRoutingTable,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Calculate depth coverage
        let mut depth_coverage: HashMap<u8, usize> = HashMap::new();
        
        for (depth, peers) in &routing_table.peers_by_depth {
            depth_coverage.insert(*depth, peers.len());
        }
        
        routing_table.topology_state.depth_coverage = depth_coverage;
        
        // Calculate network diameter
        let max_depth = routing_table.peers_by_depth.keys().max().copied().unwrap_or(0);
        routing_table.topology_state.network_diameter = max_depth;
        
        // Calculate distribution quality
        let total_peers: usize = routing_table.peers_by_depth.values().map(|v| v.len()).sum();
        let expected_per_depth = total_peers / (max_depth as usize + 1);
        
        let mut quality_sum = 0.0;
        for depth in 0..=max_depth {
            let actual = routing_table.peers_by_depth.get(&depth).map(|v| v.len()).unwrap_or(0);
            let quality = 1.0 - ((actual as f64 - expected_per_depth as f64).abs() / expected_per_depth as f64);
            quality_sum += quality.max(0.0);
        }
        
        routing_table.topology_state.distribution_quality = quality_sum / (max_depth as f64 + 1.0);
        
        Ok(())
    }

    /// Get optimal peers for specific operation
    pub async fn get_optimal_peers(
        &self,
        operation: &OperationType,
        shard_id: Option<ShardId>,
    ) -> Vec<FractalPeerInfo> {
        let routing_table = self.routing_table.read().await;
        let mut candidates = Vec::new();
        
        match operation {
            OperationType::BlockPropagation => {
                // Select peers with high bandwidth and low latency
                candidates = self.select_high_performance_peers(&routing_table).await;
            }
            OperationType::TransactionRelay => {
                // Select peers with good transaction relay performance
                candidates = self.select_relay_peers(&routing_table).await;
            }
            OperationType::ConsensusParticipation => {
                // Select peers participating in consensus
                candidates = self.select_consensus_peers(&routing_table).await;
            }
            OperationType::StateSync => {
                // Select peers with specific shard responsibility
                if let Some(shard) = shard_id {
                    candidates = self.discover_shard_peers(shard).await;
                }
            }
        }
        
        // Sort by quality score
        candidates.sort_by(|a, b| b.quality_score.partial_cmp(&a.quality_score).unwrap());
        candidates.truncate(MAX_PEERS_PER_SHARD);
        
        candidates
    }

    /// Select high-performance peers
    async fn select_high_performance_peers(
        &self,
        routing_table: &FractalRoutingTable,
    ) -> Vec<FractalPeerInfo> {
        let mut peers = Vec::new();
        
        for peer_list in routing_table.peers_by_depth.values() {
            for peer in peer_list {
                if let Some(quality) = routing_table.peer_quality.get(&peer.peer_id) {
                    let performance_score = (quality.bandwidth_score * 0.5) + 
                                         ((1000.0 - quality.latency_ms as f64) / 1000.0 * 0.3) + 
                                         (quality.reliability_score * 0.2);
                    
                    if performance_score > 0.8 {
                        peers.push(peer.clone());
                    }
                }
            }
        }
        
        peers
    }

    /// Select relay peers optimized for transaction propagation
    async fn select_relay_peers(
        &self,
        routing_table: &FractalRoutingTable,
    ) -> Vec<FractalPeerInfo> {
        // Select peers with good connectivity and low latency
        let mut peers = Vec::new();
        
        for peer_list in routing_table.peers_by_depth.values() {
            for peer in peer_list {
                if peer.quality_score > 0.75 && peer.fractal_depth < 8 {
                    peers.push(peer.clone());
                }
            }
        }
        
        peers
    }

    /// Select consensus peers
    async fn select_consensus_peers(
        &self,
        routing_table: &FractalRoutingTable,
    ) -> Vec<FractalPeerInfo> {
        // Select peers with high reliability for consensus
        let mut peers = Vec::new();
        
        for peer_list in routing_table.peers_by_depth.values() {
            for peer in peer_list {
                if let Some(quality) = routing_table.peer_quality.get(&peer.peer_id) {
                    if quality.reliability_score > 0.9 {
                        peers.push(peer.clone());
                    }
                }
            }
        }
        
        peers
    }

    /// Handle Kademlia events
    pub async fn handle_kademlia_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::RoutingUpdated { peer, addresses, .. } => {
                self.update_peer_routing(peer, addresses).await;
            }
            KademliaEvent::UnroutablePeer { peer, .. } => {
                self.handle_unroutable_peer(peer).await;
            }
            _ => {}
        }
    }

    /// Handle mDNS events
    pub async fn handle_mdns_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(peers) => {
                for (peer_id, multiaddr) in peers {
                    self.add_discovered_peer(peer_id, vec![multiaddr]).await;
                }
            }
            MdnsEvent::Expired(peers) => {
                for (peer_id, _) in peers {
                    self.remove_peer(peer_id).await;
                }
            }
        }
    }

    /// Update peer routing information
    async fn update_peer_routing(&mut self, peer_id: PeerId, addresses: Vec<Multiaddr>) {
        let mut routing_table = self.routing_table.write().await;
        
        // Find peer and update addresses
        for peers in routing_table.peers_by_depth.values_mut() {
            for peer in peers {
                if peer.peer_id == peer_id {
                    peer.addresses = addresses.clone();
                    peer.last_seen = current_timestamp();
                    return;
                }
            }
        }
        
        // Add new peer if not found
        let new_peer = FractalPeerInfo {
            peer_id,
            addresses,
            shard_responsibility: Vec::new(),
            quality_score: 0.5, // Default score
            last_seen: current_timestamp(),
            fractal_depth: 0,
        };
        
        routing_table.peers_by_depth.entry(0)
            .or_insert_with(Vec::new)
            .push(new_peer);
    }

    /// Handle unroutable peer
    async fn handle_unroutable_peer(&mut self, peer_id: PeerId) {
        let mut routing_table = self.routing_table.write().await;
        
        // Mark peer as low quality
        if let Some(quality) = routing_table.peer_quality.get_mut(&peer_id) {
            quality.reliability_score *= 0.8;
        }
    }

    /// Add discovered peer
    async fn add_discovered_peer(&mut self, peer_id: PeerId, addresses: Vec<Multiaddr>) {
        let mut routing_table = self.routing_table.write().await;
        
        let new_peer = FractalPeerInfo {
            peer_id,
            addresses,
            shard_responsibility: Vec::new(),
            quality_score: 0.6, // Slightly higher than default for discovered peers
            last_seen: current_timestamp(),
            fractal_depth: 0,
        };
        
        routing_table.peers_by_depth.entry(0)
            .or_insert_with(Vec::new)
            .push(new_peer);
    }

    /// Remove peer from routing table
    async fn remove_peer(&mut self, peer_id: PeerId) {
        let mut routing_table = self.routing_table.write().await;
        
        // Remove from peers_by_depth
        for peers in routing_table.peers_by_depth.values_mut() {
            peers.retain(|p| p.peer_id != peer_id);
        }
        
        // Remove from peer_quality
        routing_table.peer_quality.remove(&peer_id);
    }

    /// Get network topology summary
    pub async fn get_topology_summary(&self) -> TopologySummary {
        let routing_table = self.routing_table.read().await;
        
        TopologySummary {
            total_peers: routing_table.peers_by_depth.values().map(|v| v.len()).sum(),
            depth_coverage: routing_table.topology_state.depth_coverage.clone(),
            network_diameter: routing_table.topology_state.network_diameter,
            distribution_quality: routing_table.topology_state.distribution_quality,
        }
    }
}

/// Operation types for peer selection
#[derive(Debug, Clone)]
pub enum OperationType {
    BlockPropagation,
    TransactionRelay,
    ConsensusParticipation,
    StateSync,
}

/// Network topology summary
#[derive(Debug, Clone)]
pub struct TopologySummary {
    pub total_peers: usize,
    pub depth_coverage: HashMap<u8, usize>,
    pub network_diameter: u8,
    pub distribution_quality: f64,
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
    async fn test_fractal_discovery_creation() {
        let local_peer_id = PeerId::random();
        let discovery = FractalDiscovery::new(local_peer_id).unwrap();
        
        let summary = discovery.get_topology_summary().await;
        assert_eq!(summary.total_peers, 0);
    }

    #[tokio::test]
    async fn test_peer_quality_update() {
        let local_peer_id = PeerId::random();
        let mut discovery = FractalDiscovery::new(local_peer_id).unwrap();
        
        let peer_id = PeerId::random();
        discovery.update_peer_quality(peer_id, 50, 0.9).await.unwrap();
        
        let routing_table = discovery.routing_table.read().await;
        assert!(routing_table.peer_quality.contains_key(&peer_id));
    }

    #[tokio::test]
    async fn test_optimal_peer_selection() {
        let local_peer_id = PeerId::random();
        let discovery = FractalDiscovery::new(local_peer_id).unwrap();
        
        let peers = discovery.get_optimal_peers(&OperationType::BlockPropagation, None).await;
        assert!(peers.len() <= MAX_PEERS_PER_SHARD);
    }
}
