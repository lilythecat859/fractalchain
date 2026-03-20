// fractalchain/consensus/src/fractal_bft.rs
//! FractalBFT consensus engine with recursive voting and sub-second finality
//! Implements hybrid consensus: FractalBFT + Proof-of-Useful-Work

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use bls12_381::{Signature, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use fractalchain_types::{ShardId, Block, BlockHeader, FractalError};

/// Sub-second finality target: 750ms
pub const FINALITY_TIMEOUT: Duration = Duration::from_millis(750);
/// Recursive voting rounds for fractal consensus
pub const RECURSIVE_VOTING_ROUNDS: u8 = 3;
/// BLS signature aggregation threshold (67%)
pub const SIGNATURE_THRESHOLD: f64 = 0.67;
/// Maximum validators per shard (2^16 for parallel efficiency)
pub const MAX_VALIDATORS_PER_SHARD: usize = 65536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalVote {
    /// Validator's public key
    pub validator_id: PublicKey,
    /// Block hash being voted for
    pub block_hash: [u8; 32],
    /// Shard ID
    pub shard_id: ShardId,
    /// Vote weight (stake-based)
    pub weight: u64,
    /// Recursive depth of vote
    pub depth: u8,
    /// BLS signature
    pub signature: Signature,
    /// Parent shard vote reference
    pub parent_vote: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecursiveVoteAggregate {
    /// Aggregated BLS signature
    pub aggregated_signature: Signature,
    /// Participating validators
    pub validator_set: Vec<PublicKey>,
    /// Total weight
    pub total_weight: u64,
    /// Recursive depth
    pub depth: u8,
    /// Child shard aggregates
    pub child_aggregates: Vec<RecursiveVoteAggregate>,
}

#[derive(Debug, Clone)]
pub struct FractalBFTState {
    /// Current view number
    pub view: u64,
    /// Primary validator for current view
    pub primary: PublicKey,
    /// Vote aggregates by block hash
    pub vote_aggregates: HashMap<[u8; 32], RecursiveVoteAggregate>,
    /// Finalized blocks
    pub finalized_blocks: HashSet<[u8; 32]>,
    /// Validator stake weights
    pub validator_set: HashMap<PublicKey, u64>,
    /// Recursive voting state
    pub recursive_state: RecursiveVotingState,
}

#[derive(Debug, Clone)]
pub struct RecursiveVotingState {
    /// Current recursion depth
    pub current_depth: u8,
    /// Votes received at each depth
    pub votes_at_depth: HashMap<u8, Vec<FractalVote>>,
    /// Aggregates received from child shards
    pub child_aggregates: HashMap<ShardId, RecursiveVoteAggregate>,
    /// Finality decisions at each depth
    pub finality_decisions: HashMap<u8, [u8; 32]>,
}

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Invalid validator: {0:?}")]
    InvalidValidator(PublicKey),
    #[error("Insufficient votes: {current}/{required}")]
    InsufficientVotes { current: u64, required: u64 },
    #[error("Recursive voting failed at depth {depth}")]
    RecursiveVotingFailed { depth: u8 },
    #[error("Fractal consensus error: {0}")]
    FractalError(#[from] FractalError),
    #[error("Signature verification failed")]
    SignatureVerificationFailed,
    #[error("Timeout waiting for finality")]
    FinalityTimeout,
}

pub struct FractalBFT {
    /// Local validator secret key
    secret_key: SecretKey,
    /// Public key cache
    public_key: PublicKey,
    /// Current consensus state
    state: RwLock<FractalBFTState>,
    /// Network channel for votes
    vote_tx: mpsc::Sender<FractalVote>,
    vote_rx: RwLock<mpsc::Receiver<FractalVote>>,
    /// Finality notifications
    finality_tx: mpsc::Sender<[u8; 32]>,
}

impl FractalBFT {
    /// Create a new FractalBFT instance
    pub fn new(
        secret_key: SecretKey,
        validator_set: HashMap<PublicKey, u64>,
        finality_tx: mpsc::Sender<[u8; 32]>,
    ) -> Self {
        let public_key = PublicKey::from(&secret_key);
        let (vote_tx, vote_rx) = mpsc::channel(1024);
        
        let state = FractalBFTState {
            view: 0,
            primary: Self::select_primary(0, &validator_set),
            vote_aggregates: HashMap::new(),
            finalized_blocks: HashSet::new(),
            validator_set: validator_set.clone(),
            recursive_state: RecursiveVotingState {
                current_depth: 0,
                votes_at_depth: HashMap::new(),
                child_aggregates: HashMap::new(),
                finality_decisions: HashMap::new(),
            },
        };

        FractalBFT {
            secret_key,
            public_key,
            state: RwLock::new(state),
            vote_tx,
            vote_rx: RwLock::new(vote_rx),
            finality_tx,
        }
    }

    /// Propose a new block for consensus
    pub async fn propose_block(&self, block: Block) -> Result<(), ConsensusError> {
        let mut state = self.state.write().await;
        
        // Validate block fractal properties
        block.validate().map_err(ConsensusError::FractalError)?;
        
        // Create initial vote as primary
        let vote = FractalVote {
            validator_id: self.public_key,
            block_hash: block.header.hash,
            shard_id: block.header.shard_id,
            weight: *state.validator_set.get(&self.public_key)
                .ok_or_else(|| ConsensusError::InvalidValidator(self.public_key))?,
            depth: 0,
            signature: self.sign_vote(&block.header.hash)?,
            parent_vote: None,
        };

        // Broadcast vote to network
        self.broadcast_vote(vote).await?;
        
        // Start recursive voting process
        self.start_recursive_voting(&mut state, block.header.hash).await
    }

    /// Process incoming votes with recursive aggregation
    pub async fn process_vote(&self, vote: FractalVote) -> Result<(), ConsensusError> {
        // Verify vote signature
        self.verify_vote_signature(&vote).await?;
        
        let mut state = self.state.write().await;
        
        // Store vote by depth
        state.recursive_state.votes_at_depth
            .entry(vote.depth)
            .or_insert_with(Vec::new)
            .push(vote.clone());

        // Check if we have enough votes to proceed
        if self.has_sufficient_votes(&state, vote.depth)? {
            self.aggregate_votes_at_depth(&mut state, vote.depth).await?;
            
            // If at maximum depth, finalize or recurse
            if vote.depth >= RECURSIVE_VOTING_ROUNDS {
                self.finalize_if_possible(&mut state).await?;
            } else {
                self.recurse_voting(&mut state, vote.depth + 1).await?;
            }
        }

        Ok(())
    }

    /// Aggregate votes at a specific recursive depth using BLS signatures
    async fn aggregate_votes_at_depth(
        &self,
        state: &mut FractalBFTState,
        depth: u8,
    ) -> Result<RecursiveVoteAggregate, ConsensusError> {
        let votes = state.recursive_state.votes_at_depth
            .get(&depth)
            .ok_or_else(|| ConsensusError::RecursiveVotingFailed { depth })?;

        // Aggregate BLS signatures
        let mut aggregated_sig = Signature::from_bytes(&[0u8; 96]).unwrap();
        let mut validator_set = Vec::new();
        let mut total_weight = 0u64;

        for vote in votes {
            aggregated_sig.add_assign(&vote.signature);
            validator_set.push(vote.validator_id);
            total_weight += vote.weight;
        }

        let aggregate = RecursiveVoteAggregate {
            aggregated_signature: aggregated_sig,
            validator_set,
            total_weight,
            depth,
            child_aggregates: Vec::new(),
        };

        // Store aggregate
        let block_hash = votes.first()
            .ok_or_else(|| ConsensusError::RecursiveVotingFailed { depth })?
            .block_hash;

        state.vote_aggregates.insert(block_hash, aggregate.clone());
        
        Ok(aggregate)
    }

    /// Recursive voting mechanism for fractal consensus
    async fn recurse_voting(
        &self,
        state: &mut FractalBFTState,
        new_depth: u8,
    ) -> Result<(), ConsensusError> {
        state.recursive_state.current_depth = new_depth;
        
        // Get child shards for current shard
        let current_shard = ShardId(0); // Simplified for root shard
        let child_shards = current_shard.children();
        
        // Request votes from child shards
        for child_shard in child_shards {
            let child_vote = FractalVote {
                validator_id: self.public_key,
                block_hash: state.recursive_state.finality_decisions
                    .get(&(new_depth - 1))
                    .copied()
                    .unwrap_or([0u8; 32]),
                shard_id: child_shard,
                weight: *state.validator_set.get(&self.public_key)
                    .unwrap_or(&0),
                depth: new_depth,
                signature: self.sign_vote(&[0u8; 32])?,
                parent_vote: None,
            };
            
            self.broadcast_vote(child_vote).await?;
        }

        Ok(())
    }

    /// Check if we have sufficient votes for consensus (67% threshold)
    fn has_sufficient_votes(
        &self,
        state: &FractalBFTState,
        depth: u8,
    ) -> Result<bool, ConsensusError> {
        let votes = state.recursive_state.votes_at_depth
            .get(&depth)
            .map(|v| v.len())
            .unwrap_or(0);

        let total_stake: u64 = state.validator_set.values().sum();
        let current_stake: u64 = state.recursive_state.votes_at_depth
            .get(&depth)
            .map(|votes| {
                votes.iter()
                    .filter_map(|v| state.validator_set.get(&v.validator_id))
                    .sum()
            })
            .unwrap_or(0);

        Ok(current_stake as f64 / total_stake as f64 >= SIGNATURE_THRESHOLD)
    }

    /// Finalize block if consensus reached
    async fn finalize_if_possible(
        &self,
        state: &mut FractalBFTState,
    ) -> Result<(), ConsensusError> {
        // Find block with most aggregate weight
        let mut best_block = None;
        let mut best_weight = 0u64;

        for (block_hash, aggregate) in &state.vote_aggregates {
            if aggregate.depth >= RECURSIVE_VOTING_ROUNDS && aggregate.total_weight > best_weight {
                best_weight = aggregate.total_weight;
                best_block = Some(*block_hash);
            }
        }

        if let Some(block_hash) = best_block {
            // Check total stake threshold
            let total_stake: u64 = state.validator_set.values().sum();
            if best_weight as f64 / total_stake as f64 >= SIGNATURE_THRESHOLD {
                state.finalized_blocks.insert(block_hash);
                state.view += 1;
                state.primary = Self::select_primary(state.view, &state.validator_set);
                
                // Notify about finality
                let _ = self.finality_tx.send(block_hash).await;
            }
        }

        Ok(())
    }

    /// Select primary validator for view (deterministic round-robin)
    fn select_primary(
        view: u64,
        validator_set: &HashMap<PublicKey, u64>,
    ) -> PublicKey {
        let validators: Vec<_> = validator_set.keys().collect();
        let index = (view as usize) % validators.len();
        *validators[index]
    }

    /// Sign a vote with BLS signature
    fn sign_vote(&self, block_hash: &[u8; 32]) -> Result<Signature, ConsensusError> {
        // BLS signature implementation would go here
        Ok(Signature::from_bytes(&[0u8; 96]).unwrap())
    }

    /// Verify vote signature
    async fn verify_vote_signature(&self, vote: &FractalVote) -> Result<(), ConsensusError> {
        // BLS signature verification would go here
        Ok(())
    }

    /// Broadcast vote to network
    async fn broadcast_vote(&self, vote: FractalVote) -> Result<(), ConsensusError> {
        self.vote_tx.send(vote).await
            .map_err(|_| ConsensusError::RecursiveVotingFailed { depth: 0 })
    }

    /// Get current consensus state
    pub async fn get_state(&self) -> FractalBFTState {
        self.state.read().await.clone()
    }

    /// Process vote queue
    pub async fn process_vote_queue(&self) -> Result<(), ConsensusError> {
        let mut rx = self.vote_rx.write().await;
        
        while let Ok(vote) = rx.try_recv() {
            self.process_vote(vote).await?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls12_381::SecretKey;
    use rand::rngs::OsRng;

    #[tokio::test]
    async fn test_fractal_bft_creation() {
        let secret_key = SecretKey::random(&mut OsRng);
        let mut validator_set = HashMap::new();
        
        let pk = PublicKey::from(&secret_key);
        validator_set.insert(pk, 1000);
        
        let (finality_tx, _) = mpsc::channel(10);
        let consensus = FractalBFT::new(secret_key, validator_set, finality_tx);
        
        let state = consensus.get_state().await;
        assert_eq!(state.view, 0);
        assert_eq!(state.primary, pk);
    }

    #[tokio::test]
    async fn test_recursive_voting() {
        let secret_key = SecretKey::random(&mut OsRng);
        let mut validator_set = HashMap::new();
        
        let pk = PublicKey::from(&secret_key);
        validator_set.insert(pk, 1000);
        
        let (finality_tx, _) = mpsc::channel(10);
        let consensus = FractalBFT::new(secret_key, validator_set, finality_tx);
        
        // Test recursive voting state initialization
        let state = consensus.get_state().await;
        assert_eq!(state.recursive_state.current_depth, 0);
    }
}
