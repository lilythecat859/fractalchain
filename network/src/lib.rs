// fractalchain/network/src/lib.rs
//! Fractal networking layer with recursive gossip and peer discovery

pub mod fractal_gossip;
pub mod peer_discovery;

pub use fractal_gossip::{FractalGossipProtocol, FractalMessage, MessageType, NetworkError};
pub use peer_discovery::{FractalDiscovery, FractalPeerInfo, OperationType};
