use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub address: SocketAddr,
    pub reputation: f64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PNetwork {
    pub local_id: String,
    pub peers: HashMap<String, Peer>,
    pub max_peers: usize,
    pub listen_address: SocketAddr,
}

impl P2PNetwork {
    pub fn new(local_id: String, listen_address: SocketAddr, max_peers: usize) -> Self {
        P2PNetwork {
            local_id,
            peers: HashMap::new(),
            max_peers,
            listen_address,
        }
    }
    
    pub fn add_peer(&mut self, peer: Peer) -> bool {
        if self.peers.len() >= self.max_peers {
            return false;
        }
        
        self.peers.insert(peer.id.clone(), peer);
        true
    }
    
    pub fn remove_peer(&mut self, peer_id: &str) -> Option<Peer> {
        self.peers.remove(peer_id)
    }
    
    pub fn get_peer(&self, peer_id: &str) -> Option<&Peer> {
        self.peers.get(peer_id)
    }
    
    pub fn get_peer_count(&self) -> usize {
        self.peers.len()
    }
    
    pub fn broadcast(&self, message: &str) {
        // In a real implementation, we'd broadcast to all peers
        println!("📡 Broadcasting to {} peers: {}", self.peers.len(), message);
    }
    
    pub fn get_connected_peers(&self) -> Vec<String> {
        self.peers.keys().cloned().collect()
    }
}