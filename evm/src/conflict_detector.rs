// fractalchain/evm/src/conflict_detector.rs
//! Conflict detection for parallel EVM execution
//! Implements read/write set tracking and cross-shard dependency analysis

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use fractalchain_types::{ShardId, Transaction};

/// Read/write set tracking for conflict detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessSet {
    /// Storage locations read
    pub reads: HashSet<[u8; 32]>,
    /// Storage locations written
    pub writes: HashSet<[u8; 32]>,
    /// Balance accesses
    pub balance_accesses: HashSet<[u8; 20]>,
    /// Code accesses
    pub code_accesses: HashSet<[u8; 20]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictAnalysis {
    /// Direct conflicts (read-after-write, write-after-read, write-after-write)
    pub direct_conflicts: Vec<Conflict>,
    /// Cross-shard dependencies
    pub cross_shard_deps: Vec<CrossShardDependency>,
    /// Conflict severity score (0-1)
    pub conflict_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    /// First transaction involved
    pub tx1_hash: [u8; 32],
    /// Second transaction involved
    pub tx2_hash: [u8; 32],
    /// Conflicting state key
    pub state_key: [u8; 32],
    /// Conflict type
    pub conflict_type: ConflictType,
    /// Shard involvement
    pub shards: (ShardId, ShardId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictType {
    ReadAfterWrite,
    WriteAfterRead,
    WriteAfterWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardDependency {
    /// Source shard
    pub source_shard: ShardId,
    /// Target shard
    pub target_shard: ShardId,
    /// Dependency type
    pub dep_type: DependencyType,
    /// State keys involved
    pub state_keys: Vec<[u8; 32]>,
    /// Dependency strength (0-1)
    pub strength: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DependencyType {
    Strong,  // Must be resolved before continuation
    Weak,    // Can be resolved asynchronously
    Deferred, // Can be resolved after execution
}

pub struct ConflictDetector {
    /// Global access tracking across all shards
    global_access_map: HashMap<[u8; 32], Vec<AccessRecord>>,
    /// Cross-shard dependency graph
    dependency_graph: HashMap<ShardId, Vec<ShardDependency>>,
    /// Conflict history for ML-based prediction
    conflict_history: Vec<ConflictRecord>,
}

#[derive(Debug, Clone)]
struct AccessRecord {
    tx_hash: [u8; 32],
    shard_id: ShardId,
    access_type: AccessType,
    timestamp: u64,
}

#[derive(Debug, Clone)]
enum AccessType {
    Read,
    Write,
}

#[derive(Debug, Clone)]
struct ShardDependency {
    dependent_shard: ShardId,
    dependency_type: DependencyType,
    confidence: f64,
}

#[derive(Debug, Clone)]
struct ConflictRecord {
    tx1: [u8; 32],
    tx2: [u8; 32],
    conflict_type: ConflictType,
    resolution: ConflictResolution,
    timestamp: u64,
}

#[derive(Debug, Clone)]
enum ConflictResolution {
    Reordered,
    Sequentialized,
    Aborted,
}

impl ConflictDetector {
    /// Create a new conflict detector
    pub fn new() -> Self {
        ConflictDetector {
            global_access_map: HashMap::new(),
            dependency_graph: HashMap::new(),
            conflict_history: Vec::new(),
        }
    }

    /// Analyze transactions for conflicts before execution
    pub fn analyze_conflicts(
        &self,
        transactions: &[Transaction],
        shard_assignment: &HashMap<[u8; 32], ShardId>,
    ) -> ConflictAnalysis {
        let mut access_sets = HashMap::new();
        let mut direct_conflicts = Vec::new();
        let mut cross_shard_deps = Vec::new();

        // Build access sets for each transaction
        for tx in transactions {
            let access_set = self.build_access_set(tx);
            access_sets.insert(tx.hash, access_set);
        }

        // Detect direct conflicts
        for (i, tx1) in transactions.iter().enumerate() {
            for tx2 in transactions.iter().skip(i + 1) {
                if let Some(conflict) = self.detect_direct_conflict(
                    tx1,
                    tx2,
                    &access_sets[&tx1.hash],
                    &access_sets[&tx2.hash],
                    shard_assignment,
                ) {
                    direct_conflicts.push(conflict);
                }
            }
        }

        // Detect cross-shard dependencies
        cross_shard_deps = self.detect_cross_shard_dependencies(
            transactions,
            &access_sets,
            shard_assignment,
        );

        // Calculate conflict score
        let conflict_score = self.calculate_conflict_score(&direct_conflicts, &cross_shard_deps);

        ConflictAnalysis {
            direct_conflicts,
            cross_shard_deps,
            conflict_score,
        }
    }

    /// Build access set for a transaction (static analysis)
    fn build_access_set(&self, tx: &Transaction) -> AccessSet {
        let mut access_set = AccessSet {
            reads: HashSet::new(),
            writes: HashSet::new(),
            balance_accesses: HashSet::new(),
            code_accesses: HashSet::new(),
        };

        // Analyze transaction data for state accesses
        if !tx.data.is_empty() {
            // Simplified analysis - real implementation would use proper EVM analysis
            self.analyze_contract_calls(&tx.data, &mut access_set);
        }

        // Always access sender and recipient balances
        access_set.balance_accesses.insert(tx.from);
        if let Some(to) = tx.to {
            access_set.balance_accesses.insert(to);
        }

        // Nonce access for sender
        access_set.reads.insert(self.derive_nonce_key(&tx.from));

        access_set
    }

    /// Analyze contract calls for state accesses
    fn analyze_contract_calls(&self, data: &[u8], access_set: &mut AccessSet) {
        if data.len() < 4 {
            return;
        }

        let selector = &data[0..4];
        
        // Common function selectors (simplified)
        match selector {
            [0xa9, 0x05, 0x9c, 0xbb] => { // transfer(address,uint256)
                // Access recipient balance
                if data.len() >= 24 {
                    let recipient = &data[4..24];
                    access_set.balance_accesses.insert(Self::bytes_to_address(recipient));
                }
            }
            [0x23, 0xb8, 0x72, 0xdd] => { // transferFrom(address,address,uint256)
                // Access both sender and recipient balances
                if data.len() >= 44 {
                    let sender = &data[4..24];
                    let recipient = &data[24..44];
                    access_set.balance_accesses.insert(Self::bytes_to_address(sender));
                    access_set.balance_accesses.insert(Self::bytes_to_address(recipient));
                }
            }
            _ => {
                // Generic contract call - mark as potentially accessing storage
                access_set.writes.insert([0xFFu8; 32]); // Wildcard
            }
        }
    }

    /// Detect direct conflicts between two transactions
    fn detect_direct_conflict(
        &self,
        tx1: &Transaction,
        tx2: &Transaction,
        set1: &AccessSet,
        set2: &AccessSet,
        shard_assignment: &HashMap<[u8; 32], ShardId>,
    ) -> Option<Conflict> {
        let shard1 = shard_assignment.get(&tx1.hash)?;
        let shard2 = shard_assignment.get(&tx2.hash)?;
        
        // Check storage conflicts
        for key in &set1.reads {
            if set2.writes.contains(key) {
                return Some(Conflict {
                    tx1_hash: tx1.hash,
                    tx2_hash: tx2.hash,
                    state_key: *key,
                    conflict_type: ConflictType::ReadAfterWrite,
                    shards: (*shard1, *shard2),
                });
            }
        }

        for key in &set1.writes {
            if set2.reads.contains(key) {
                return Some(Conflict {
                    tx1_hash: tx1.hash,
                    tx2_hash: tx2.hash,
                    state_key: *key,
                    conflict_type: ConflictType::WriteAfterRead,
                    shards: (*shard1, *shard2),
                });
            }
            if set2.writes.contains(key) {
                return Some(Conflict {
                    tx1_hash: tx1.hash,
                    tx2_hash: tx2.hash,
                    state_key: *key,
                    conflict_type: ConflictType::WriteAfterWrite,
                    shards: (*shard1, *shard2),
                });
            }
        }

        // Check balance conflicts
        for addr in &set1.balance_accesses {
            if set2.balance_accesses.contains(addr) {
                let key = Self::derive_balance_key(addr);
                return Some(Conflict {
                    tx1_hash: tx1.hash,
                    tx2_hash: tx2.hash,
                    state_key: key,
                    conflict_type: ConflictType::WriteAfterWrite,
                    shards: (*shard1, *shard2),
                });
            }
        }

        None
    }

    /// Detect cross-shard dependencies
    fn detect_cross_shard_dependencies(
        &self,
        transactions: &[Transaction],
        access_sets: &HashMap<[u8; 32], AccessSet>,
        shard_assignment: &HashMap<[u8; 32], ShardId>,
    ) -> Vec<CrossShardDependency> {
        let mut deps = Vec::new();
        let mut shard_access_map: HashMap<ShardId, Vec<[u8; 32]>> = HashMap::new();

        // Group state accesses by shard
        for tx in transactions {
            let shard_id = shard_assignment[&tx.hash];
            let access_set = &access_sets[&tx.hash];
            
            let mut state_keys = Vec::new();
            state_keys.extend(access_set.reads.iter().copied());
            state_keys.extend(access_set.writes.iter().copied());
            
            shard_access_map.entry(shard_id)
                .or_insert_with(Vec::new)
                .extend(state_keys);
        }

        // Find cross-shard dependencies
        for (shard1, keys1) in &shard_access_map {
            for (shard2, keys2) in &shard_access_map {
                if shard1 >= shard2 { continue; }
                
                let common_keys: Vec<[u8; 32]> = keys1.iter()
                    .filter(|k| keys2.contains(k))
                    .copied()
                    .collect();
                
                if !common_keys.is_empty() {
                    let strength = common_keys.len() as f64 / keys1.len().max(keys2.len()) as f64;
                    
                    deps.push(CrossShardDependency {
                        source_shard: *shard1,
                        target_shard: *shard2,
                        state_keys: common_keys,
                        dep_type: if strength > 0.7 {
                            DependencyType::Strong
                        } else {
                            DependencyType::Weak
                        },
                        strength,
                    });
                }
            }
        }

        deps
    }

    /// Calculate conflict score based on detected conflicts
    fn calculate_conflict_score(
        &self,
        direct_conflicts: &[Conflict],
        cross_shard_deps: &[CrossShardDependency],
    ) -> f64 {
        let direct_score = (direct_conflicts.len() as f64 * 0.3).min(1.0);
        let cross_shard_score = (cross_shard_deps.len() as f64 * 0.2).min(1.0);
        
        (direct_score + cross_shard_score).min(1.0)
    }

    /// Predict conflicts using ML-based analysis of conflict history
    pub fn predict_conflicts(&self, transactions: &[Transaction]) -> Vec<Conflict> {
        let mut predicted_conflicts = Vec::new();
        
        // Simple ML model based on historical conflict patterns
        for (i, tx1) in transactions.iter().enumerate() {
            for tx2 in transactions.iter().skip(i + 1) {
                let conflict_probability = self.calculate_conflict_probability(tx1, tx2);
                
                if conflict_probability > 0.7 {
                    // Predict conflict based on historical patterns
                    predicted_conflicts.push(Conflict {
                        tx1_hash: tx1.hash,
                        tx2_hash: tx2.hash,
                        state_key: [0u8; 32], // Generic key
                        conflict_type: ConflictType::WriteAfterWrite,
                        shards: (ShardId(0), ShardId(1)),
                    });
                }
            }
        }
        
        predicted_conflicts
    }

    /// Calculate conflict probability based on historical data
    fn calculate_conflict_probability(&self, tx1: &Transaction, tx2: &Transaction) -> f64 {
        // Look for similar transaction pairs in history
        let mut similar_conflicts = 0usize;
        let mut total_similar = 0usize;
        
        for record in &self.conflict_history {
            if self.transactions_similar(&record.tx1, &tx1.hash) && 
               self.transactions_similar(&record.tx2, &tx2.hash) {
                total_similar += 1;
                if matches!(record.conflict_type, ConflictType::WriteAfterWrite) {
                    similar_conflicts += 1;
                }
            }
        }
        
        if total_similar == 0 {
            0.0
        } else {
            similar_conflicts as f64 / total_similar as f64
        }
    }

    /// Check if two transactions are similar (simplified)
    fn transactions_similar(&self, tx1_hash: &[u8; 32], tx2_hash: &[u8; 32]) -> bool {
        tx1_hash[0..16] == tx2_hash[0..16]
    }

    /// Suggest transaction reordering to minimize conflicts
    pub fn suggest_reordering(&self, transactions: &mut Vec<Transaction>) -> Vec<usize> {
        // Simple topological sort based on dependencies
        let mut suggested_order = Vec::new();
        let mut remaining: Vec<_> = (0..transactions.len()).collect();
        
        while !remaining.is_empty() {
            // Find transaction with minimal conflicts
            let mut best_idx = 0;
            let mut min_conflicts = usize::MAX;
            
            for (i, &tx_idx) in remaining.iter().enumerate() {
                let conflicts = self.count_potential_conflicts(&transactions[tx_idx], transactions);
                if conflicts < min_conflicts {
                    min_conflicts = conflicts;
                    best_idx = i;
                }
            }
            
            suggested_order.push(remaining.remove(best_idx));
        }
        
        // Reorder transactions
        let original = transactions.clone();
        for (i, &idx) in suggested_order.iter().enumerate() {
            transactions[i] = original[idx].clone();
        }
        
        suggested_order
    }

    /// Count potential conflicts for a transaction
    fn count_potential_conflicts(&self, tx: &Transaction, all_txs: &[Transaction]) -> usize {
        let mut count = 0;
        
        for other_tx in all_txs {
            if tx.hash != other_tx.hash && self.transactions_will_conflict(tx, other_tx) {
                count += 1;
            }
        }
        
        count
    }

    /// Check if two transactions will conflict (simplified heuristic)
    fn transactions_will_conflict(&self, tx1: &Transaction, tx2: &Transaction) -> bool {
        // Check if they access similar addresses
        let tx1_addrs = self.get_involved_addresses(tx1);
        let tx2_addrs = self.get_involved_addresses(tx2);
        
        !tx1_addrs.is_disjoint(&tx2_addrs)
    }

    /// Get addresses involved in a transaction
    fn get_involved_addresses(&self, tx: &Transaction) -> HashSet<[u8; 20]> {
        let mut addrs = HashSet::new();
        addrs.insert(tx.from);
        if let Some(to) = tx.to {
            addrs.insert(to);
        }
        addrs
    }

    /// Helper functions
    fn derive_nonce_key(address: &[u8; 20]) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..20].copy_from_slice(address);
        key[20..32].copy_from_slice(b"nonce_______");
        key
    }

    fn derive_balance_key(address: &[u8; 20]) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..20].copy_from_slice(address);
        key[20..32].copy_from_slice(b"balance_____");
        key
    }

    fn bytes_to_address(bytes: &[u8]) -> [u8; 20] {
        let mut addr = [0u8; 20];
        addr.copy_from_slice(&bytes[0..20]);
        addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_set_creation() {
        let detector = ConflictDetector::new();
        let tx = Transaction::new(
            [0xAAu8; 20],
            Some([0xBBu8; 20]),
            1000000000000000000,
            21000,
            20000000000,
            0,
            vec![0xa9, 0x05, 0x9c, 0xbb, 0x00; 24], // transfer function
            859,
            ShardId(1),
            ShardId(1),
        );

        let access_set = detector.build_access_set(&tx);
        assert!(access_set.balance_accesses.contains(&[0xAAu8; 20]));
        assert!(access_set.balance_accesses.contains(&[0xBBu8; 20]));
    }

    #[test]
    fn test_conflict_detection() {
        let detector = ConflictDetector::new();
        let tx1 = Transaction::new(
            [0xAAu8; 20],
            Some([0xBBu8; 20]),
            1000000000000000000,
            21000,
            20000000000,
            0,
            vec![],
            859,
            ShardId(1),
            ShardId(1),
        );

        let tx2 = Transaction::new(
            [0xCCu8; 20],
            Some([0xBBu8; 20]), // Same recipient - conflict
            500000000000000000,
            21000,
            20000000000,
            0,
            vec![],
            859,
            ShardId(2),
            ShardId(2),
        );

        let mut assignment = HashMap::new();
        assignment.insert(tx1.hash, ShardId(1));
        assignment.insert(tx2.hash, ShardId(2));

        let analysis = detector.analyze_conflicts(&[tx1, tx2], &assignment);
        assert!(!analysis.direct_conflicts.is_empty());
        assert_eq!(analysis.direct_conflicts[0].conflict_type, ConflictType::WriteAfterWrite);
    }

    #[test]
    fn test_cross_shard_dependency_detection() {
        let detector = ConflictDetector::new();
        let tx1 = Transaction::new(
            [0xAAu8; 20],
            Some([0xBBu8; 20]),
            1000000000000000000,
            21000,
            20000000000,
            0,
            vec![],
            859,
            ShardId(1),
            ShardId(2), // Cross-shard
        );

        let tx2 = Transaction::new(
            [0xBBu8; 20],
            Some([0xCCu8; 20]),
            500000000000000000,
            21000,
            20000000000,
            0,
            vec![],
            859,
            ShardId(2),
            ShardId(2),
        );

        let mut assignment = HashMap::new();
        assignment.insert(tx1.hash, ShardId(1));
        assignment.insert(tx2.hash, ShardId(2));

        let analysis = detector.analyze_conflicts(&[tx1, tx2], &assignment);
        assert!(!analysis.cross_shard_deps.is_empty());
    }
}
