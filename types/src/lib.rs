// fractalchain/types/src/lib.rs
//! Core types for FRACTALCHAIN
//! Implements fractal mathematics and core data structures

pub mod fractal;
pub mod block;
pub mod transaction;
pub mod state;

pub use fractal::{FractalShardId, FRACTAL_DIMENSION, MAX_FRACTAL_DEPTH};

// fractalchain/network/src/lib.rs
//! Network layer for FRACTALCHAIN
//! Implements libp2p-based gossipsub with fractal routing

pub mod fractal_gossip;
pub mod peer_discovery;
pub mod connection_manager;

pub use fractal_gossip::{FractalGossip, FractalMessage, RoutingStats};
pub use peer_discovery::{PeerDiscovery, DiscoveryEvent};
pub use connection_manager::{ConnectionManager, ConnectionEvent};

// fractalchain/network/src/peer_discovery.rs
//! Peer discovery service for fractal network topology

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde::{Serialize, Deserialize};
use libp2p::{PeerId, Multiaddr};
use libp2p::kad::{Kademlia, KademliaConfig, KademliaEvent, QueryResult};
use libp2p::kad::store::MemoryStore;
use fractalchain_types::FractalShardId;

/// Peer information for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub shard_id: FractalShardId,
    pub addresses: Vec<Multiaddr>,
    pub capabilities: Vec<String>,
    pub last_seen: u64,
    pub reputation: f64,
}

impl PeerInfo {
    pub fn new(peer_id: PeerId, shard_id: FractalShardId, addresses: Vec<Multiaddr>) -> Self {
        Self {
            peer_id,
            shard_id,
            addresses,
            capabilities: vec!["gossip".to_string(), "consensus".to_string()],
            last_seen: current_timestamp_ms(),
            reputation: 1.0,
        }
    }

    /// Update reputation based on behavior
    pub fn update_reputation(&mut self, success: bool) {
        if success {
            self.reputation = (self.reputation * 0.9 + 1.0 * 0.1).min(1.0);
        } else {
            self.reputation = (self.reputation * 0.9).max(0.0);
        }
        self.last_seen = current_timestamp_ms();
    }

    /// Check if peer is stale
    pub fn is_stale(&self) -> bool {
        current_timestamp_ms() - self.last_seen > 600000 // 10 minutes
    }
}

/// Discovery events
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// New peer discovered
    PeerDiscovered(PeerInfo),
    /// Peer updated
    PeerUpdated(PeerInfo),
    /// Peer lost
    PeerLost(PeerId),
    /// Discovery complete
    DiscoveryComplete,
}

/// Peer discovery service
#[derive(Debug)]
pub struct PeerDiscovery {
    /// Kademlia DHT instance
    pub kademlia: Kademlia<MemoryStore>,
    /// Known peers by shard
    pub peers_by_shard: Arc<RwLock<HashMap<FractalShardId, Vec<PeerInfo>>>>,
    /// Peer information cache
    pub peer_cache: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    /// Local shard ID
    pub local_shard_id: FractalShardId,
    /// Discovery interval
    pub discovery_interval: Duration,
}

impl PeerDiscovery {
    pub fn new(local_peer_id: PeerId, local_shard_id: FractalShardId) -> Self {
        let mut kademlia_config = KademliaConfig::default();
        kademlia_config.set_query_timeout(Duration::from_secs(30));
        
        let store = MemoryStore::new(local_peer_id);
        let kademlia = Kademlia::with_config(local_peer_id, store, kademlia_config);
        
        Self {
            kademlia,
            peers_by_shard: Arc::new(RwLock::new(HashMap::new())),
            peer_cache: Arc::new(RwLock::new(HashMap::new())),
            local_shard_id,
            discovery_interval: Duration::from_secs(60),
        }
    }

    /// Start discovery process
    pub fn start_discovery(&mut self) -> Result<Vec<DiscoveryEvent>, String> {
        let mut events = vec![];
        
        // Discover peers in local shard
        events.extend(self.discover_shard_peers(self.local_shard_id)?);
        
        // Discover peers in parent shards
        let mut current_shard = self.local_shard_id;
        for depth in 0..4 {
            if let Some(parent) = current_shard.parent() {
                events.extend(self.discover_shard_peers(parent)?);
                current_shard = parent;
            } else {
                break;
            }
        }
        
        // Discover peers in child shards
        let children = self.local_shard_id.children();
        for child in children {
            events.extend(self.discover_shard_peers(child)?);
        }
        
        Ok(events)
    }

    /// Discover peers for specific shard
    fn discover_shard_peers(&mut self, shard_id: FractalShardId) -> Result<Vec<DiscoveryEvent>, String> {
        let mut events = vec![];
        
        // Generate shard-specific key for DHT lookup
        let shard_key = format!("fractal:shard:{}", shard_id);
        
        // Perform DHT lookup
        let query_id = self.kademlia.get_closest_peers(shard_key.into_bytes());
        
        // In a real implementation, this would be asynchronous
        // For now, we simulate finding peers
        let discovered_peers = self.simulate_peer_discovery(shard_id, 5);
        
        for peer_info in discovered_peers {
            events.push(DiscoveryEvent::PeerDiscovered(peer_info));
        }
        
        Ok(events)
    }

    /// Simulate peer discovery (for testing)
    fn simulate_peer_discovery(&self, shard_id: FractalShardId, count: usize) -> Vec<PeerInfo> {
        let mut peers = vec![];
        
        for i in 0..count {
            let peer_id = PeerId::random();
            let addr = format!("/ip4/127.0.0.1/tcp/{}", 4000 + i).parse().unwrap();
            
            let peer_info = PeerInfo::new(peer_id, shard_id, vec![addr]);
            peers.push(peer_info);
        }
        
        peers
    }

    /// Handle Kademlia events
    pub fn handle_kademlia_event(&mut self, event: KademliaEvent) -> Result<Vec<DiscoveryEvent>, String> {
        let mut events = vec![];
        
        match event {
            KademliaEvent::OutboundQueryCompleted { result, .. } => {
                match result {
                    QueryResult::GetClosestPeers(Ok(peers)) => {
                        for peer in peers.peers {
                            // Check if we have peer info
                            let peer_info = self.get_peer_info(&peer);
                            if let Some(info) = peer_info {
                                events.push(DiscoveryEvent::PeerDiscovered(info));
                            }
                        }
                    },
                    QueryResult::GetProviders(Ok(providers)) => {
                        for provider in providers.providers {
                            let peer_info = self.get_peer_info(&provider);
                            if let Some(info) = peer_info {
                                events.push(DiscoveryEvent::PeerDiscovered(info));
                            }
                        }
                    },
                    _ => {},
                }
            },
            KademliaEvent::RoutingUpdated { peer, addresses, .. } => {
                // Update peer information
                let peer_info = PeerInfo::new(peer, self.local_shard_id, addresses.to_vec());
                events.push(DiscoveryEvent::PeerUpdated(peer_info));
            },
            _ => {},
        }
        
        Ok(events)
    }

    /// Get peer information
    fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        let cache = self.peer_cache.read().unwrap();
        cache.get(peer_id).cloned()
    }

    /// Add peer to cache
    pub fn add_peer(&self, peer_info: PeerInfo) -> Result<(), String> {
        let mut cache = self.peer_cache.write().unwrap();
        let mut by_shard = self.peers_by_shard.write().unwrap();
        
        // Update cache
        cache.insert(peer_info.peer_id, peer_info.clone());
        
        // Update shard mapping
        let shard_peers = by_shard.entry(peer_info.shard_id).or_insert_with(Vec::new);
        
        // Remove old entry if exists
        shard_peers.retain(|p| p.peer_id != peer_info.peer_id);
        shard_peers.push(peer_info);
        
        Ok(())
    }

    /// Get peers for specific shard
    pub fn get_shard_peers(&self, shard_id: FractalShardId, limit: usize) -> Vec<PeerInfo> {
        let by_shard = self.peers_by_shard.read().unwrap();
        
        if let Some(peers) = by_shard.get(&shard_id) {
            let mut result: Vec<_> = peers.iter()
                .filter(|p| !p.is_stale())
                .take(limit)
                .cloned()
                .collect();
            
            // Sort by reputation
            result.sort_by(|a, b| b.reputation.partial_cmp(&a.reputation).unwrap());
            result
        } else {
            vec![]
        }
    }

    /// Get all peers
    pub fn get_all_peers(&self) -> Vec<PeerInfo> {
        let cache = self.peer_cache.read().unwrap();
        cache.values().cloned().collect()
    }

    /// Remove stale peers
    pub fn cleanup_stale_peers(&self) -> Result<usize, String> {
        let mut cache = self.peer_cache.write().unwrap();
        let mut by_shard = self.peers_by_shard.write().unwrap();
        
        let initial_count = cache.len();
        
        // Remove stale peers from cache
        cache.retain(|_, peer| !peer.is_stale());
        
        // Remove stale peers from shard mappings
        for peers in by_shard.values_mut() {
            peers.retain(|peer| !peer.is_stale());
        }
        
        // Remove empty shard entries
        by_shard.retain(|_, peers| !peers.is_empty());
        
        let removed_count = initial_count - cache.len();
        Ok(removed_count)
    }

    /// Bootstrap with known peers
    pub fn bootstrap(&mut self, bootstrap_peers: Vec<(PeerId, Multiaddr)>) -> Result<(), String> {
        for (peer_id, addr) in bootstrap_peers {
            self.kademlia.add_address(&peer_id, addr);
        }
        
        // Start bootstrap process
        self.kademlia.bootstrap().map_err(|e| format!("Bootstrap failed: {:?}", e))?;
        
        Ok(())
    }

    /// Announce local presence
    pub fn announce_presence(&mut self) -> Result<(), String> {
        let shard_key = format!("fractal:shard:{}", self.local_shard_id);
        
        // Announce in DHT
        self.kademlia.put_record(
            libp2p::kad::Record::new(shard_key.into_bytes(), vec![]),
            libp2p::kad::Quorum::One,
        ).map_err(|e| format!("Failed to announce presence: {:?}", e))?;
        
        Ok(())
    }

    /// Get discovery statistics
    pub fn get_stats(&self) -> DiscoveryStats {
        let cache = self.peer_cache.read().unwrap();
        let by_shard = self.peers_by_shard.read().unwrap();
        
        let total_peers = cache.len();
        let total_shards = by_shard.len();
        
        let active_peers = cache.values()
            .filter(|peer| !peer.is_stale())
            .count();
        
        let avg_reputation = if total_peers > 0 {
            cache.values().map(|peer| peer.reputation).sum::<f64>() / total_peers as f64
        } else {
            0.0
        };
        
        DiscoveryStats {
            total_peers,
            total_shards,
            active_peers,
            avg_reputation,
            local_shard: self.local_shard_id,
        }
    }
}

/// Discovery statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoveryStats {
    pub total_peers: usize,
    pub total_shards: usize,
    pub active_peers: usize,
    pub avg_reputation: f64,
    pub local_shard: FractalShardId,
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::PeerId;

    #[test]
    fn test_peer_info_creation() {
        let peer_id = PeerId::random();
        let shard_id = FractalShardId::root();
        let addr = "/ip4/127.0.0.1/tcp/4000".parse().unwrap();
        
        let peer_info = PeerInfo::new(peer_id, shard_id, vec![addr]);
        
        assert_eq!(peer_info.shard_id, shard_id);
        assert_eq!(peer_info.reputation, 1.0);
        assert!(!peer_info.is_stale());
    }

    #[test]
    fn test_peer_reputation_update() {
        let peer_id = PeerId::random();
        let shard_id = FractalShardId::root();
        let mut peer_info = PeerInfo::new(peer_id, shard_id, vec![]);
        
        peer_info.update_reputation(true);
        assert!(peer_info.reputation > 0.9);
        
        peer_info.update_reputation(false);
        assert!(peer_info.reputation < 0.9);
    }

    #[test]
    fn test_peer_discovery_creation() {
        let local_peer_id = PeerId::random();
        let local_shard_id = FractalShardId::root();
        let discovery = PeerDiscovery::new(local_peer_id, local_shard_id);
        
        assert_eq!(discovery.local_shard_id, local_shard_id);
        assert!(discovery.peers_by_shard.read().unwrap().is_empty());
    }

    #[test]
    fn test_shard_peer_retrieval() {
        let local_peer_id = PeerId::random();
        let local_shard_id = FractalShardId::root();
        let discovery = PeerDiscovery::new(local_peer_id, local_shard_id);
        
        // Add test peers
        for i in 0..5 {
            let peer_id = PeerId::random();
            let peer_info = PeerInfo::new(peer_id, local_shard_id, vec![]);
            discovery.add_peer(peer_info).unwrap();
        }
        
        let peers = discovery.get_shard_peers(local_shard_id, 3);
        assert_eq!(peers.len(), 3);
    }

    #[test]
    fn test_stale_peer_cleanup() {
        let local_peer_id = PeerId::random();
        let local_shard_id = FractalShardId::root();
        let discovery = PeerDiscovery::new(local_peer_id, local_shard_id);
        
        // Add stale peer
        let mut stale_peer = PeerInfo::new(PeerId::random(), local_shard_id, vec![]);
        stale_peer.last_seen = current_timestamp_ms() - 700000; // 11+ minutes ago
        
        discovery.add_peer(stale_peer).unwrap();
        
        // Cleanup
        let removed = discovery.cleanup_stale_peers().unwrap();
        assert_eq!(removed, 1);
        
        let peers = discovery.get_all_peers();
        assert!(peers.is_empty());
    }
}

