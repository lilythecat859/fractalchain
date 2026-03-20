// fractalchain/types/src/state.rs
//! State management with Verkle trees and fractal sharding

use serde::{Serialize, Deserialize};
use crate::fractal::FractalShardId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub nonce: u64,
    pub balance: u128,
    pub storage_root: [u8; 32],
    pub code_hash: [u8; 32],
}

impl Default for Account {
    fn default() -> Self {
        Self {
            nonce: 0,
            balance: 0,
            storage_root: [0u8; 32],
            code_hash: [0u8; 32],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEntry {
    pub address: [u8; 20],
    pub account: Account,
    pub shard_id: FractalShardId,
}

impl StateEntry {
    pub fn new(address: [u8; 20], account: Account) -> Self {
        let shard_id = FractalShardId::shard_for_address(&address);
        Self {
            address,
            account,
            shard_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDiff {
    pub shard_id: FractalShardId,
        pub changes: Vec<StateChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateChange {
    pub address: [u8; 20],
    pub old_value: Option<Account>,
    pub new_value: Option<Account>,
}

impl StateDiff {
    pub fn new(shard_id: FractalShardId) -> Self {
        Self {
            shard_id,
            changes: vec![],
        }
    }

    pub fn add_change(&mut self, address: [u8; 20], old: Option<Account>, new: Option<Account>) {
        self.changes.push(StateChange {
            address,
            old_value: old,
            new_value: new,
        });
    }

    /// Apply this state diff to a state root
    pub fn apply(&self, state_root: &mut [u8; 32]) -> Result<(), String> {
        // Verkle tree update logic would go here
        // For now, we just update the root hash
        use blake3::Hasher;
        let mut hasher = Hasher::new();
        
        hasher.update(state_root);
        hasher.update(&self.shard_id.0.to_be_bytes());
        hasher.update(&self.changes.len().to_be_bytes());
        
        *state_root = *hasher.finalize().as_bytes();
        Ok(())
    }
}

/// Fractal state manager with Verkle tree support
#[derive(Debug)]
pub struct FractalState {
    /// Shard ID for this state manager
    pub shard_id: FractalShardId,
    /// Current state root
    pub state_root: [u8; 32],
    /// State entries cached in memory
    cache: std::collections::HashMap<[u8; 20], Account>,
}

impl FractalState {
    pub fn new(shard_id: FractalShardId) -> Self {
        Self {
            shard_id,
            state_root: [0u8; 32],
            cache: std::collections::HashMap::new(),
        }
    }

    /// Get account from state
    pub fn get_account(&self, address: &[u8; 20]) -> Option<&Account> {
        self.cache.get(address)
    }

    /// Update account in state
    pub fn update_account(&mut self, address: [u8; 20], account: Account) -> Result<(), String> {
        // Verify address belongs to this shard
        let address_shard = FractalShardId::shard_for_address(&address);
        if address_shard != self.shard_id {
            return Err(format!(
                "Address {} does not belong to shard {:?}",
                hex::encode(address),
                self.shard_id
            ));
        }

        self.cache.insert(address, account);
        Ok(())
    }

    /// Apply a state diff to this state
    pub fn apply_diff(&mut self, diff: &StateDiff) -> Result<(), String> {
        if diff.shard_id != self.shard_id {
            return Err("State diff for wrong shard".to_string());
        }

        for change in &diff.changes {
            match (&change.old_value, &change.new_value) {
                (None, Some(new)) => {
                    // Creation
                    self.cache.insert(change.address, new.clone());
                },
                (Some(_), Some(new)) => {
                    // Update
                    self.cache.insert(change.address, new.clone());
                },
                (Some(_), None) => {
                    // Deletion
                    self.cache.remove(&change.address);
                },
                (None, None) => {
                    // No-op
                },
            }
        }

        // Update state root
        diff.apply(&mut self.state_root)?;
        Ok(())
    }

    /// Calculate state root hash
    pub fn calculate_root(&self) -> [u8; 32] {
        use blake3::Hasher;
        let mut hasher = Hasher::new();
        
        let mut addresses: Vec<_> = self.cache.keys().collect();
        addresses.sort();
        
        for address in addresses {
            hasher.update(address);
            if let Some(account) = self.cache.get(address) {
                hasher.update(&account.nonce.to_be_bytes());
                hasher.update(&account.balance.to_be_bytes());
                hasher.update(&account.storage_root);
                hasher.update(&account.code_hash);
            }
        }
        
        *hasher.finalize().as_bytes()
    }

    /// Get state diff since last root calculation
    pub fn diff_since(&self, old_root: &[u8; 32]) -> StateDiff {
        let current_root = self.calculate_root();
        
        if &current_root == old_root {
            return StateDiff::new(self.shard_id);
        }

        // For now, return full state as diff
        // In production, this would track actual changes
        let mut diff = StateDiff::new(self.shard_id);
        
        for (address, account) in &self.cache {
            diff.add_change(*address, None, Some(account.clone()));
        }
        
        diff
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_creation() {
        let account = Account::default();
        assert_eq!(account.nonce, 0);
        assert_eq!(account.balance, 0);
    }

    #[test]
    fn test_state_entry() {
        let address = [0x42u8; 20];
        let account = Account {
            nonce: 1,
            balance: 1000000000000000000,
            ..Default::default()
        };
        
        let entry = StateEntry::new(address, account.clone());
        assert_eq!(entry.address, address);
        assert_eq!(entry.account.nonce, 1);
    }

    #[test]
    fn test_fractal_state() {
        let shard = FractalShardId::root();
        let mut state = FractalState::new(shard);
        
        let address = [0x42u8; 20];
        let account = Account {
            nonce: 1,
            balance: 1000000000000000000,
            ..Default::default()
        };
        
        state.update_account(address, account.clone()).unwrap();
        
        let retrieved = state.get_account(&address).unwrap();
        assert_eq!(retrieved.nonce, 1);
        assert_eq!(retrieved.balance, 1000000000000000000);
    }

    #[test]
    fn test_state_diff() {
        let shard = FractalShardId::root();
        let mut state = FractalState::new(shard);
        
        let address = [0x42u8; 20];
        let account = Account {
            nonce: 1,
            balance: 1000000000000000000,
            ..Default::default()
        };
        
        state.update_account(address, account).unwrap();
        
        let old_root = [0u8; 32];
        let diff = state.diff_since(&old_root);
        
        assert_eq!(diff.shard_id, shard);
        assert_eq!(diff.changes.len(), 1);
    }
}

