// fractalchain/rpc/src/lib.rs
//! Ethereum-compatible JSON-RPC implementation with fractal extensions

pub mod eth_compatibility;
pub mod fractal_rpc;

pub use eth_compatibility::{EthRpcServer, EthApiServer, FractalApiServer};
pub use fractal_rpc::{FractalDebugRpcServer, FractalDebugApiServer};