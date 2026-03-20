// fractalchain/consensus/src/lib.rs
//! FractalBFT consensus engine with Proof-of-Useful-Work
//! Provides sub-second finality and recursive voting

pub mod fractal_bft;
pub mod pouw;

pub use fractal_bft::{FractalBFT, FractalVote, RecursiveVoteAggregate, ConsensusError};
pub use pouw::{PoUWEngine, PoUWSolution, MandelbrotProof};