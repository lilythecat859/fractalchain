// fractalchain/state/src/lib.rs
//! State management with Verkle trees and state expiry for fractal sharding

pub mod verkle_tree;
pub mod state_expiry;

pub use verkle_tree::{VerkleTree, VerkleProof, FractalCoordinates};
pub use state_expiry::{StateExpiryManager, ExpiryStats, ArchiveReference};
