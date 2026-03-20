use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Validator {
    pub address: String,
    pub stake: u64,
    pub reputation: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Consensus {
    pub validators: HashMap<String, Validator>,
    pub epoch: u64,
    pub finality_threshold: f64,
    pub votes: Vec<Vote>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    pub validator: String,
    pub block_hash: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusState {
    pub votes: Vec<Vote>,
    pub finalized_blocks: Vec<String>,
    pub current_leader: Option<String>,
}

impl Consensus {
    pub fn new(validators: Vec<String>) -> Self {
        let mut validator_map = HashMap::new();
        
        for addr in validators {
            validator_map.insert(addr.clone(), Validator {
                address: addr,
                stake: 1000,
                reputation: 1.0,
            });
        }
        
        Consensus {
            validators: validator_map,
            epoch: 0,
            finality_threshold: 0.67,
            votes: Vec::new(),
        }
    }
    
    pub fn vote(&mut self, validator: String, block_hash: String) -> bool {
        if !self.validators.contains_key(&validator) {
            return false;
        }
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        let vote = Vote {
            validator: validator.clone(),
            block_hash: block_hash.clone(),
            timestamp,
        };
        
        // In a real implementation, we'd implement proper consensus logic
        true
    }
    
    pub fn is_finalized(&self, block_hash: &str) -> bool {
        // Count votes for this block
        let votes_for_block = self.votes.iter()
            .filter(|vote| vote.block_hash == block_hash)
            .count();
            
        let total_validators = self.validators.len();
        let required_votes = (total_validators as f64 * self.finality_threshold) as usize;
        
        votes_for_block >= required_votes
    }
    
    pub fn get_leader(&self) -> Option<String> {
        // Simple round-robin leader selection
        let validators: Vec<String> = self.validators.keys().cloned().collect();
        if validators.is_empty() {
            None
        } else {
            let leader_index = self.epoch as usize % validators.len();
            Some(validators[leader_index].clone())
        }
    }
    
    pub fn next_epoch(&mut self) {
        self.epoch += 1;
    }
    
    pub fn get_stake(&self, validator: &str) -> Option<u64> {
        self.validators.get(validator).map(|v| v.stake)
    }
}