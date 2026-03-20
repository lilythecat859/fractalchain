use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::blockchain::{Transaction, Block, Blockchain};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub shard_id: u32,
    pub blockchain: Blockchain,
    pub pending_transactions: Vec<Transaction>,
}

impl Shard {
    pub fn new(shard_id: u32) -> Self {
        Shard {
            shard_id,
            blockchain: Blockchain::new(),
            pending_transactions: Vec::new(),
        }
    }
    
    pub fn add_transaction(&mut self, mut transaction: Transaction) {
        // Assign transaction to this shard
        transaction.shard_id = self.shard_id;
        self.pending_transactions.push(transaction);
    }
    
    pub fn mine_block(&mut self) -> Option<Block> {
        if self.pending_transactions.is_empty() {
            return None;
        }
        
        // Add pending transactions to blockchain
        for tx in &self.pending_transactions {
            self.blockchain.add_transaction(tx.clone());
        }
        
        // Mine a block
        let new_block = self.blockchain.mine_block();
        
        if new_block.is_some() {
            self.pending_transactions.clear();
        }
        
        new_block
    }
    
    pub fn get_stats(&self) -> ShardStats {
        ShardStats {
            shard_id: self.shard_id,
            blocks: self.blockchain.get_height(),
            transactions: self.blockchain.chain.iter()
                .map(|block| block.transactions.len() as u64)
                .sum(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalChain {
    pub shards: HashMap<u32, Shard>,
    pub num_shards: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStats {
    pub shard_id: u32,
    pub blocks: u64,
    pub transactions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStats {
    pub total_blocks: u64,
    pub total_transactions: u64,
    pub active_shards: u32,
    pub avg_block_time: f64,
}

impl FractalChain {
    pub fn new(num_shards: u32) -> Self {
        let mut shards = HashMap::new();
        
        for i in 0..num_shards {
            shards.insert(i, Shard::new(i));
        }
        
        FractalChain {
            shards,
            num_shards,
        }
    }
    
    pub fn add_transaction(&mut self, mut transaction: Transaction) {
        // Route transaction to appropriate shard based on ID
        let shard_id = (transaction.id % self.num_shards as u64) as u32;
        
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            shard.add_transaction(transaction);
        }
    }
    
    pub fn mine_all_shards(&mut self) {
        for i in 0..self.num_shards {
            if let Some(shard) = self.shards.get_mut(&i) {
                shard.mine_block();
            }
        }
    }
    
    pub fn get_stats(&self) -> ChainStats {
        let total_blocks: u64 = self.shards.values()
            .map(|shard| shard.blockchain.get_height())
            .sum();
            
        let total_transactions: u64 = self.shards.values()
            .map(|shard| shard.get_stats().transactions)
            .sum();
            
        ChainStats {
            total_blocks,
            total_transactions,
            active_shards: self.num_shards,
            avg_block_time: 2.0, // Simulated
        }
    }
    
    pub fn get_shard_stats(&self, shard_id: u32) -> Option<ShardStats> {
        self.shards.get(&shard_id).map(|shard| shard.get_stats())
    }
}