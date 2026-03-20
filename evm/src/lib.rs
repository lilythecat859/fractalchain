// fractalchain/evm/src/lib.rs
//! Parallel EVM execution engine with fractal sharding support

pub mod parallel_executor;
pub mod conflict_detector;
pub mod state;

pub use parallel_executor::{ParallelEvmExecutor, ExecutionResult, ExecutionError};
pub use conflict_detector::{ConflictDetector, ConflictAnalysis};
pub use state::{EvmState, StateProof};
