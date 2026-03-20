// fractalchain/cmd/src/genesis.rs
//! Genesis block configuration and bootstrapping logic
//! Implements zero-cost startup with fair launch mechanics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use sha2::{Sha256, Digest};

use fractalchain_types::{Block, BlockHeader, Transaction, ShardId, FractalError};
use fractalchain_consensus::{FractalBFT, FractalVote};
use fractalchain_evm::EvmState;

/// Genesis timestamp: February 15, 2026 00:00:00 UTC
pub const GENESIS_TIMESTAMP: u64 = 1776000000;
/// Chain ID: 859 (homage to lilythecat859)
pub const CHAIN_ID: u64 = 859;
/// Genesis gas limit: 30M
pub const GENESIS_GAS_LIMIT: u64 = 30_000_000;
/// Fair launch addresses: first 1000 unique addresses
pub const FAIR_LAUNCH_ADDRESSES: usize = 1000;
/// Genesis block reward: 1 FRAC
pub const GENESIS_BLOCK_REWARD: u64 = 1_000_000_000_000_000_000; // 1 ETH in wei
/// Fractal base shards: 2^16
pub const FRACTAL_BASE_SHARDS: u64 = 65536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisConfig {
    /// Genesis timestamp
    pub timestamp: u64,
    /// Chain ID
    pub chain_id: u64,
    /// Genesis block hash
    pub genesis_hash: [u8; 32],
    /// Initial state root
    pub initial_state_root: [u8; 32],
    /// Fair launch participants
    pub fair_launch_participants: Vec<FairLaunchParticipant>,
    /// Initial shard distribution
    pub shard_distribution: HashMap<ShardId, GenesisShardInfo>,
    /// Consensus parameters
    pub consensus_params: GenesisConsensusParams,
    /// Economic parameters
    pub economic_params: GenesisEconomicParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairLaunchParticipant {
    /// Participant address
    pub address: [u8; 20],
    /// Initial balance (1 FRAC)
    pub balance: u128,
    /// Assigned shard
    pub shard_id: ShardId,
    /// Participation proof
    pub participation_proof: [u8; 32],
    /// Fractal coordinate
    pub fractal_coordinate: FractalCoordinate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalCoordinate {
    pub x: f64,
    pub y: f64,
    pub depth: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisShardInfo {
    /// Shard ID
    pub shard_id: ShardId,
    /// Initial state root for this shard
    pub state_root: [u8; 32],
    /// Number of participants
    pub participant_count: u64,
    /// Total balance
    pub total_balance: u128,
    /// Fractal depth
    pub fractal_depth: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisConsensusParams {
    /// Initial validator set
    pub initial_validators: Vec<[u8; 48]>, // BLS public keys
    /// Consensus threshold (67%)
    pub consensus_threshold: f64,
    /// Block time target (250ms)
    pub block_time_ms: u64,
    /// Finality timeout (750ms)
    pub finality_timeout_ms: u64,
    /// Recursive voting rounds
    pub recursive_voting_rounds: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisEconomicParams {
    /// Initial base fee (1 Gwei)
    pub initial_base_fee: u128,
    /// Block reward (1 FRAC)
    pub block_reward: u128,
    /// Emission rate (0.5% annual)
    pub emission_rate: f64,
    /// Gas price target ($0.0001 per transfer)
    pub gas_price_target: u128,
    /// Minimum gas price
    pub min_gas_price: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisBuilder {
    /// Genesis configuration
    config: GenesisConfig,
    /// Participant counter for fair launch
    participant_counter: usize,
    /// Fractal topology builder
    topology_builder: FractalTopologyBuilder,
}

#[derive(Debug, Clone)]
pub struct FractalTopologyBuilder {
    /// Shard assignments based on fractal mathematics
    shard_assignments: HashMap<[u8; 20], ShardId>,
    /// Fractal coordinates for each shard
    fractal_coordinates: HashMap<ShardId, FractalCoordinate>,
    /// Hausdorff dimension calculations
    hausdorff_dimensions: HashMap<ShardId, f64>,
}

impl GenesisConfig {
    /// Create default genesis configuration
    pub fn default() -> Self {
        let timestamp = GENESIS_TIMESTAMP;
        let chain_id = CHAIN_ID;
        
        // Generate genesis hash
        let genesis_hash = Self::calculate_genesis_hash(timestamp, chain_id);
        
        // Build fair launch participants
        let fair_launch_participants = Self::generate_fair_launch_participants();
        
        // Build initial shard distribution
        let shard_distribution = Self::build_shard_distribution(&fair_launch_participants);
        
        // Calculate initial state root
        let initial_state_root = Self::calculate_initial_state_root(&shard_distribution);
        
        GenesisConfig {
            timestamp,
            chain_id,
            genesis_hash,
            initial_state_root,
            fair_launch_participants,
            shard_distribution,
            consensus_params: GenesisConsensusParams {
                initial_validators: Self::generate_initial_validators(),
                consensus_threshold: 0.67,
                block_time_ms: 250,
                finality_timeout_ms: 750,
                recursive_voting_rounds: 3,
            },
            economic_params: GenesisEconomicParams {
                initial_base_fee: 1000000000, // 1 Gwei
                block_reward: GENESIS_BLOCK_REWARD,
                emission_rate: 0.005, // 0.5% annual
                gas_price_target: 100000000000, // 100 Gwei for $0.0001
                min_gas_price: 1000000000, // 1 Gwei
            },
        }
    }

    /// Calculate deterministic genesis hash
    fn calculate_genesis_hash(timestamp: u64, chain_id: u64) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&chain_id.to_le_bytes());
        hasher.update(b"FRACTALCHAIN_GENESIS");
        hasher.finalize().into()
    }

    /// Generate fair launch participants using fractal mathematics
    fn generate_fair_launch_participants() -> Vec<FairLaunchParticipant> {
        let mut participants = Vec::new();
        
        for i in 0..FAIR_LAUNCH_ADDRESSES {
            // Generate deterministic address from index
            let address = Self::generate_deterministic_address(i);
            
            // Calculate fractal coordinate using Mandelbrot set
            let fractal_coordinate = Self::calculate_fractal_coordinate(i);
            
            // Assign to optimal shard based on fractal properties
            let shard_id = Self::assign_optimal_shard(&address, &fractal_coordinate);
            
            // Generate participation proof (simplified)
            let participation_proof = Self::generate_participation_proof(&address, i);
            
            participants.push(FairLaunchParticipant {
                address,
                balance: GENESIS_BLOCK_REWARD,
                shard_id,
                participation_proof,
                fractal_coordinate,
            });
        }
        
        participants
    }

    /// Generate deterministic address from index
    fn generate_deterministic_address(index: usize) -> [u8; 20] {
        let mut hasher = Sha256::new();
        hasher.update(&index.to_le_bytes());
        hasher.update(b"FRACTAL_ADDRESS");
        
        let hash = hasher.finalize();
        let mut address = [0u8; 20];
        address.copy_from_slice(&hash[0..20]);
        address
    }

    /// Calculate fractal coordinate using Mandelbrot set properties
    fn calculate_fractal_coordinate(index: usize) -> FractalCoordinate {
        // Normalize index to Mandelbrot bounds
        let normalized = (index as f64) / (FAIR_LAUNCH_ADDRESSES as f64);
        let x = normalized * 3.5 - 2.5; // Mandelbrot x range [-2.5, 1.0]
        let y = (normalized * 2.0 % 1.0) * 3.0 - 1.5; // Mandelbrot y range [-1.5, 1.5]
        let depth = ((index % 256) / 8) as u8; // Fractal depth 0-31
        
        FractalCoordinate { x, y, depth }
    }

    /// Assign optimal shard based on fractal properties
    fn assign_optimal_shard(
        address: &[u8; 20],
        coordinate: &FractalCoordinate,
    ) -> ShardId {
        // Use fractal coordinate to determine shard
        let x_shard = ((coordinate.x + 2.5) / 3.5 * (FRACTAL_BASE_SHARDS as f64).sqrt()) as u64;
        let y_shard = ((coordinate.y + 1.5) / 3.0 * (FRACTAL_BASE_SHARDS as f64).sqrt()) as u64;
        
        let shard_num = (x_shard * (FRACTAL_BASE_SHARDS as f64).sqrt() as u64 + y_shard) % FRACTAL_BASE_SHARDS;
        ShardId(shard_num)
    }

    /// Generate participation proof for fair launch
    fn generate_participation_proof(address: &[u8; 20], index: usize) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(address);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"FAIR_LAUNCH_PROOF");
        hasher.finalize().into()
    }

    /// Build shard distribution from participants
    fn build_shard_distribution(
        participants: &[FairLaunchParticipant],
    ) -> HashMap<ShardId, GenesisShardInfo> {
        let mut distribution: HashMap<ShardId, GenesisShardInfo> = HashMap::new();
        
        for participant in participants {
            let info = distribution.entry(participant.shard_id).or_insert_with(|| {
                GenesisShardInfo {
                    shard_id: participant.shard_id,
                    state_root: [0u8; 32], // Will be calculated later
                    participant_count: 0,
                    total_balance: 0,
                    fractal_depth: participant.shard_id.depth(),
                }
            });
            
            info.participant_count += 1;
            info.total_balance += participant.balance;
        }
        
        // Calculate state roots for each shard
        for (shard_id, info) in distribution.iter_mut() {
            info.state_root = Self::calculate_shard_state_root(shard_id, info);
        }
        
        distribution
    }

    /// Calculate shard-specific state root
    fn calculate_shard_state_root(
        shard_id: &ShardId,
        info: &GenesisShardInfo,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&shard_id.as_u64().to_le_bytes());
        hasher.update(&info.participant_count.to_le_bytes());
        hasher.update(&info.total_balance.to_le_bytes());
        hasher.update(b"SHARD_STATE_ROOT");
        hasher.finalize().into()
    }

    /// Calculate initial global state root
    fn calculate_initial_state_root(
        shard_distribution: &HashMap<ShardId, GenesisShardInfo>,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        
        // Sort shards for deterministic calculation
        let mut shards: Vec<_> = shard_distribution.keys().collect();
        shards.sort();
        
        for shard_id in shards {
            let info = &shard_distribution[shard_id];
            hasher.update(&info.state_root);
        }
        
        hasher.finalize().into()
    }

    /// Generate initial validator set
    fn generate_initial_validators() -> Vec<[u8; 48]> {
        let mut validators = Vec::new();
        
        // Generate deterministic BLS public keys for validators
        for i in 0..64 { // 64 initial validators
            let mut key = [0u8; 48];
            for j in 0..48 {
                key[j] = ((i * 7 + j * 13) % 256) as u8;
            }
            validators.push(key);
        }
        
        validators
    }
}

impl GenesisBuilder {
    /// Create new genesis builder
    pub fn new() -> Self {
        GenesisBuilder {
            config: GenesisConfig::default(),
            participant_counter: 0,
            topology_builder: FractalTopologyBuilder::new(),
        }
    }

    /// Add fair launch participant
    pub fn add_participant(&mut self, address: [u8; 20]) -> Result<ShardId, FractalError> {
        if self.participant_counter >= FAIR_LAUNCH_ADDRESSES {
            return Err(FractalError::InvalidDepth(255)); // Using as generic error
        }
        
        // Calculate fractal coordinate
        let coordinate = GenesisConfig::calculate_fractal_coordinate(self.participant_counter);
        
        // Assign optimal shard
        let shard_id = GenesisConfig::assign_optimal_shard(&address, &coordinate);
        
        // Create participant
        let participant = FairLaunchParticipant {
            address,
            balance: GENESIS_BLOCK_REWARD,
            shard_id,
            participation_proof: GenesisConfig::generate_participation_proof(&address, self.participant_counter),
            fractal_coordinate: coordinate,
        };
        
        self.config.fair_launch_participants.push(participant);
        self.participant_counter += 1;
        
        Ok(shard_id)
    }

    /// Build genesis block
    pub fn build_genesis_block(&self) -> Result<Block, FractalError> {
        // Create genesis header
        let header = BlockHeader::new(
            0, // Block number
            [0u8; 32], // Parent hash (zero for genesis)
            [0u8; 32], // Tx root (empty)
            self.config.initial_state_root,
            ShardId(0), // Global coordination shard
            [0u8; 32], // PoW solution (zero for genesis)
        );
        
        // Verify genesis timestamp
        if header.timestamp < GENESIS_TIMESTAMP {
            return Err(FractalError::InvalidDepth(255));
        }
        
        // Create genesis block
        let genesis_block = Block::new(
            header,
            Vec::new(), // No transactions in genesis
        );
        
        Ok(genesis_block)
    }

    /// Build initial EVM state
    pub fn build_initial_state(&self) -> Result<EvmState, FractalError> {
        let mut state = EvmState::new();
        
        // Add fair launch participants to state
        for participant in &self.config.fair_launch_participants {
            // Set initial balance
            state.apply_balance_change(participant.address, participant.balance as i128);
            
            // Set initial nonce (0)
            state.set_nonce(participant.address, 0);
        }
        
        // Update Verkle commitment
        state.update_verkle_commitment();
        
        Ok(state)
    }

    /// Build initial consensus state
    pub fn build_consensus_state(&self) -> Result<FractalBFTState, FractalError> {
        // Create initial validator set
        let mut validator_set = HashMap::new();
        
        for (i, validator_key) in self.config.consensus_params.initial_validators.iter().enumerate() {
            // Convert BLS public key to our format (simplified)
            let public_key = Self::convert_bls_to_internal(validator_key);
            let stake = 1000u64; // Equal stake for all validators
            
            validator_set.insert(public_key, stake);
        }
        
        // Create initial consensus state
        let consensus_state = FractalBFTState {
            view: 0,
            primary: *validator_set.keys().next().unwrap(),
            vote_aggregates: HashMap::new(),
            finalized_blocks: HashSet::new(),
            validator_set,
            recursive_state: RecursiveVotingState {
                current_depth: 0,
                votes_at_depth: HashMap::new(),
                child_aggregates: HashMap::new(),
                finality_decisions: HashMap::new(),
            },
        };
        
        Ok(consensus_state)
    }

    /// Convert BLS public key to internal format (simplified)
    fn convert_bls_to_internal(bls_key: &[u8; 48]) -> PublicKey {
        // In real implementation, this would properly convert BLS12-381 public key
        let mut key_bytes = [0u8; 48];
        key_bytes.copy_from_slice(bls_key);
        PublicKey::from_bytes(&key_bytes).unwrap_or_default()
    }

    /// Get genesis configuration
    pub fn get_config(&self) -> &GenesisConfig {
        &self.config
    }
}

impl FractalTopologyBuilder {
    /// Create new fractal topology builder
    pub fn new() -> Self {
        FractalTopologyBuilder {
            shard_assignments: HashMap::new(),
            fractal_coordinates: HashMap::new(),
            hausdorff_dimensions: HashMap::new(),
        }
    }

    /// Add shard assignment
    pub fn add_shard_assignment(
        &mut self,
        address: [u8; 20],
        shard_id: ShardId,
        coordinate: FractalCoordinate,
    ) {
        self.shard_assignments.insert(address, shard_id);
        self.fractal_coordinates.insert(shard_id, coordinate);
        
        // Calculate Hausdorff dimension
        let dimension = Self::calculate_hausdorff_dimension(&coordinate);
        self.hausdorff_dimensions.insert(shard_id, dimension);
    }

    /// Calculate Hausdorff dimension for coordinate
    fn calculate_hausdorff_coordinate(coordinate: &FractalCoordinate) -> f64 {
        // Simplified Hausdorff dimension calculation
        // Real implementation would use proper fractal geometry
        let depth_factor = coordinate.depth as f64 / 32.0;
        2.0 * (1.0 - depth_factor * 0.5)
    }

    /// Get optimal shard for address
    pub fn get_optimal_shard(&self, address: &[u8; 20]) -> Option<ShardId> {
        self.shard_assignments.get(address).copied()
    }

    /// Get fractal coordinate for shard
    pub fn get_fractal_coordinate(&self, shard_id: &ShardId) -> Option<&FractalCoordinate> {
        self.fractal_coordinates.get(shard_id)
    }

    /// Get Hausdorff dimension for shard
    pub fn get_hausdorff_dimension(&self, shard_id: &ShardId) -> Option<f64> {
        self.hausdorff_dimensions.get(shard_id).copied()
    }
}

impl Default for GenesisBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for FractalTopologyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Genesis validation
pub fn validate_genesis(genesis: &GenesisConfig) -> Result<(), FractalError> {
    // Validate timestamp
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    if genesis.timestamp > current_time + 3600 { // 1 hour tolerance
        return Err(FractalError::InvalidDepth(255));
    }
    
    // Validate chain ID
    if genesis.chain_id != CHAIN_ID {
        return Err(FractalError::InvalidDepth(255));
    }
    
    // Validate fair launch participants
    if genesis.fair_launch_participants.len() != FAIR_LAUNCH_ADDRESSES {
        return Err(FractalError::InvalidDepth(255));
    }
    
    // Validate total supply
    let total_supply: u128 = genesis.fair_launch_participants.iter()
        .map(|p| p.balance)
        .sum();
    
    let expected_supply = FAIR_LAUNCH_ADDRESSES as u128 * GENESIS_BLOCK_REWARD;
    if total_supply != expected_supply {
        return Err(FractalError::InvalidDepth(255));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_config_creation() {
        let config = GenesisConfig::default();
        
        assert_eq!(config.timestamp, GENESIS_TIMESTAMP);
        assert_eq!(config.chain_id, CHAIN_ID);
        assert_eq!(config.fair_launch_participants.len(), FAIR_LAUNCH_ADDRESSES);
    }

    #[test]
    fn test_genesis_builder() {
        let mut builder = GenesisBuilder::new();
        
        // Add participant
        let address = [0xAAu8; 20];
        let shard_id = builder.add_participant(address).unwrap();
        
        assert!(shard_id.as_u64() < FRACTAL_BASE_SHARDS);
        
        // Build genesis block
        let genesis_block = builder.build_genesis_block().unwrap();
        assert_eq!(genesis_block.header.number, 0);
        assert!(genesis_block.header.timestamp >= GENESIS_TIMESTAMP);
    }

    #[test]
    fn test_fractal_coordinate_generation() {
        let coordinate = GenesisConfig::calculate_fractal_coordinate(42);
        
        assert!(coordinate.x >= -2.5 && coordinate.x <= 1.0);
        assert!(coordinate.y >= -1.5 && coordinate.y <= 1.5);
        assert!(coordinate.depth <= 31);
    }

    #[test]
    fn test_genesis_validation() {
        let config = GenesisConfig::default();
        
        let result = validate_genesis(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_shard_assignment() {
        let address = [0xBBu8; 20];
        let coordinate = FractalCoordinate { x: 0.0, y: 0.0, depth: 5 };
        
        let shard_id = GenesisConfig::assign_optimal_shard(&address, &coordinate);
        
        assert!(shard_id.as_u64() < FRACTAL_BASE_SHARDS);
        assert_eq!(shard_id.depth(), coordinate.depth);
    }
}