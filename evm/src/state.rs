// fractalchain/evm/src/state.rs
//! EVM state management with fractal sharding support
//! Implements stateless client architecture with Verkle trees

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

use fractalchain_types::{ShardId, FractalError};

/// Verkle tree node structure for state commitment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerkleNode {
    pub key: [u8; 32],
    pub value: Option<Vec<u8>>,
    pub commitment: [u8; 32],
    pub children: Vec<VerkleNode>,
}

/// EVM account state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub nonce: u64,
    pub balance: u128,
    pub storage_root: [u8; 32],
    pub code_hash: [u8; 32],
}

/// EVM state with fractal sharding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvmState {
    /// State roots per shard
    pub shard_states: HashMap<ShardId, ShardState>,
    /// Global state commitment (Verkle root)
    pub verkle_root: [u8; 32],
    /// State expiry tracking
    pub expiry_tracker: StateExpiryTracker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardState {
    /// Accounts in this shard
    pub accounts: HashMap<[u8; 20], Account>,
    /// Storage slots
    pub storage: HashMap<[u8; 20], HashMap<[u8; 32], [u8; 32]>>,
    /// Verkle subtree root
    pub subtree_root: [u8; 32],
    /// Last access timestamp
    pub last_access: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateExpiryTracker {
    /// State slots expiring soon (within 1 month)
    pub expiring_soon: HashMap<[u8; 32], u64>,
    /// Expired state (older than 1 year)
    pub expired: HashSet<[u8; 32]>,
    /// Archive reference (IPFS hash or similar)
    pub archive_reference: Option<[u8; 32]>,
}

use std::collections::HashSet;

impl EvmState {
    /// Create new EVM state
    pub fn new() -> Self {
        EvmState {
            shard_states: HashMap::new(),
            verkle_root: [0u8; 32],
            expiry_tracker: StateExpiryTracker {
                expiring_soon: HashMap::new(),
                expired: HashSet::new(),
                archive_reference: None,
            },
        }
    }

    /// Get account from state
    pub fn get_account(&self, address: &[u8; 20], shard_id: ShardId) -> Option<&Account> {
        self.shard_states.get(&shard_id)
            .and_then(|shard| shard.accounts.get(address))
    }

    /// Get storage value
    pub fn get_storage(&self, address: &[u8; 20], key: &[u8; 32], shard_id: ShardId) -> Option<[u8; 32]> {
        self.shard_states.get(&shard_id)
            .and_then(|shard| shard.storage.get(address))
            .and_then(|storage| storage.get(key))
            .copied()
    }

    /// Update account balance
    pub fn apply_balance_change(&mut self, address: [u8; 20], delta: i128) {
        let shard_id = self.determine_shard_for_address(&address);
        
        if let Some(shard_state) = self.shard_states.get_mut(&shard_id) {
            if let Some(account) = shard_state.accounts.get_mut(&address) {
                if delta > 0 {
                    account.balance = account.balance.saturating_add(delta as u128);
                } else {
                    account.balance = account.balance.saturating_sub((-delta) as u128);
                }
                shard_state.last_access = current_timestamp();
            }
        }
    }

    /// Set storage value
    pub fn set_storage(&mut self, address: [u8; 20], key: [u8; 32], value: [u8; 32]) {
        let shard_id = self.determine_shard_for_address(&address);
        
        let shard_state = self.shard_states.entry(shard_id)
            .or_insert_with(|| ShardState {
                accounts: HashMap::new(),
                storage: HashMap::new(),
                subtree_root: [0u8; 32],
                last_access: current_timestamp(),
            });
        
        let storage = shard_state.storage.entry(address)
            .or_insert_with(HashMap::new);
        
        storage.insert(key, value);
        
        // Update expiry tracker
        let state_key = self.derive_state_key(&address, &key);
        self.expiry_tracker.expiring_soon.insert(state_key, current_timestamp());
    }

    /// Set account nonce
    pub fn set_nonce(&mut self, address: [u8; 20], nonce: u64) {
        let shard_id = self.determine_shard_for_address(&address);
        
        let shard_state = self.shard_states.entry(shard_id)
            .or_insert_with(|| ShardState {
                accounts: HashMap::new(),
                storage: HashMap::new(),
                subtree_root: [0u8; 32],
                last_access: current_timestamp(),
            });
        
        let account = shard_state.accounts.entry(address)
            .or_insert_with(|| Account {
                nonce: 0,
                balance: 0,
                storage_root: [0u8; 32],
                code_hash: [0u8; 32],
            });
        
        account.nonce = nonce;
        shard_state.last_access = current_timestamp();
    }

    /// Determine shard for address using fractal mapping
    fn determine_shard_for_address(&self, address: &[u8; 20]) -> ShardId {
        // Use first 8 bytes of address for shard determination
        let shard_num = u64::from_le_bytes([
            address[0], address[1], address[2], address[3],
            address[4], address[5], address[6], address[7],
        ]);
        
        ShardId(shard_num % crate::fractal::SHARD_BASE)
    }

    /// Derive state key for expiry tracking
    fn derive_state_key(&self, address: &[u8; 20], storage_key: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(address);
        hasher.update(storage_key);
        hasher.finalize().into()
    }

    /// Update Verkle tree commitment
    pub fn update_verkle_commitment(&mut self) {
        // Build Verkle tree from current state
        let mut verkle_tree = VerkleNode {
            key: [0u8; 32],
            value: None,
            commitment: [0u8; 32],
            children: Vec::new(),
        };
        
        for (shard_id, shard_state) in &self.shard_states {
            self.build_verkle_subtree(&mut verkle_tree, shard_id, shard_state);
        }
        
        self.verkle_root = self.calculate_verkle_root(&verkle_tree);
    }

    /// Build Verkle subtree for a shard
    fn build_verkle_subtree(
        &self,
        parent: &mut VerkleNode,
        shard_id: &ShardId,
        shard_state: &ShardState,
    ) {
        let shard_key = self.derive_shard_key(shard_id);
        
        let mut shard_node = VerkleNode {
            key: shard_key,
            value: Some(shard_key.to_vec()),
            commitment: shard_state.subtree_root,
            children: Vec::new(),
        };
        
        // Add account nodes
        for (address, account) in &shard_state.accounts {
            self.add_account_node(&mut shard_node, address, account);
        }
        
        parent.children.push(shard_node);
    }

    /// Add account node to Verkle tree
    fn add_account_node(
        &self,
        parent: &mut VerkleNode,
        address: &[u8; 20],
        account: &Account,
    ) {
        let account_key = self.derive_account_key(address);
        
        let account_node = VerkleNode {
            key: account_key,
            value: Some(self.serialize_account(account)),
            commitment: account.storage_root,
            children: Vec::new(),
        };
        
        parent.children.push(account_node);
    }

    /// Serialize account for Verkle tree
    fn serialize_account(&self, account: &Account) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&account.nonce.to_le_bytes());
        data.extend_from_slice(&account.balance.to_le_bytes());
        data.extend_from_slice(&account.storage_root);
        data.extend_from_slice(&account.code_hash);
        data
    }

    /// Calculate Verkle tree root
    fn calculate_verkle_root(&self, tree: &VerkleNode) -> [u8; 32] {
        // Simplified - real implementation would use KZG commitments
        let mut hasher = Sha256::new();
        hasher.update(&tree.key);
        hasher.update(&tree.commitment);
        
        for child in &tree.children {
            hasher.update(&self.calculate_verkle_root(child));
        }
        
        hasher.finalize().into()
    }

    /// Derive various keys for Verkle tree
    fn derive_shard_key(&self, shard_id: &ShardId) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&shard_id.as_u64().to_le_bytes());
        key[8..32].copy_from_slice(b"shard_______________________");
        key
    }

    fn derive_account_key(&self, address: &[u8; 20]) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..20].copy_from_slice(address);
        key[20..32].copy_from_slice(b"account_____");
        key
    }

    /// Expire old state (older than 1 year)
    pub fn expire_old_state(&mut self) -> Result<Vec<[u8; 32]>, FractalError> {
        let current_time = current_timestamp();
        let one_year_seconds = 365 * 24 * 60 * 60;
        let mut expired_keys = Vec::new();
        
        // Find expired state
        for (key, timestamp) in &self.expiry_tracker.expiring_soon.clone() {
            if current_time - timestamp > one_year_seconds {
                expired_keys.push(*key);
            }
        }
        
        // Remove expired state
        for key in &expired_keys {
            self.expiry_tracker.expiring_soon.remove(key);
            self.expiry_tracker.expired.insert(*key);
        }
        
        Ok(expired_keys)
    }

    /// Get state proof for stateless clients
    pub fn get_state_proof(
        &self,
        address: &[u8; 20],
        storage_key: Option<&[u8; 32]>,
    ) -> StateProof {
        let shard_id = self.determine_shard_for_address(address);
        
        StateProof {
            shard_id,
            account_proof: self.get_account_proof(address, &shard_id),
            storage_proof: storage_key.map(|key| {
                self.get_storage_proof(address, key, &shard_id)
            }),
            verkle_root: self.verkle_root,
        }
    }

    /// Get account proof for Verkle tree
    fn get_account_proof(&self, address: &[u8; 20], shard_id: &ShardId) -> Vec<[u8; 32]> {
        // Simplified - real implementation would generate proper Merkle proof
        vec![self.derive_account_key(address)]
    }

    /// Get storage proof for Verkle tree
    fn get_storage_proof(
        &self,
        address: &[u8; 20],
        storage_key: &[u8; 32],
        shard_id: &ShardId,
    ) -> Vec<[u8; 32]> {
        // Simplified - real implementation would generate proper proof
        vec![self.derive_state_key(address, storage_key)]
    }
}

/// State proof for stateless clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateProof {
    pub shard_id: ShardId,
    pub account_proof: Vec<[u8; 32]>,
    pub storage_proof: Option<Vec<[u8; 32]>>,
    pub verkle_root: [u8; 32],
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl Default for EvmState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_creation() {
        let state = EvmState::new();
        assert_eq!(state.verkle_root, [0u8; 32]);
        assert!(state.shard_states.is_empty());
    }

    #[test]
    fn test_balance_update() {
        let mut state = EvmState::new();
        let address = [0xAAu8; 20];
        
        state.apply_balance_change(address, 1000000000000000000i128); // +1 ETH
        let shard_id = state.determine_shard_for_address(&address);
        
        let account = state.get_account(&address, shard_id);
        assert!(account.is_some());
        assert_eq!(account.unwrap().balance, 1000000000000000000);
    }

    #[test]
    fn test_storage_operations() {
        let mut state = EvmState::new();
        let address = [0xBBu8; 20];
        let key = [0xCCu8; 32];
        let value = [0xDDu8; 32];
        
        state.set_storage(address, key, value);
        let shard_id = state.determine_shard_for_address(&address);
        
        let stored_value = state.get_storage(&address, &key, shard_id);
        assert_eq!(stored_value, Some(value));
    }

    #[test]
    fn test_shard_determination() {
        let state = EvmState::new();
        let addr1 = [0x11u8; 20];
        let addr2 = [0x22u8; 20];
        
        let shard1 = state.determine_shard_for_address(&addr1);
        let shard2 = state.determine_shard_for_address(&addr2);
        
        // Should be different shards (with high probability)
        assert_ne!(shard1, shard2);
    }
}
