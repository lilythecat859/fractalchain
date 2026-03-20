use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub timestamp: u64,
    pub shard_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub index: u64,
    pub timestamp: u64,
    pub transactions: Vec<Transaction>,
    pub previous_hash: String,
    pub hash: String,
    pub nonce: u64,
}

impl Block {
    pub fn new(index: u64, transactions: Vec<Transaction>, previous_hash: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        let hash = format!("block_{}_{}_{}", index, timestamp, transactions.len());
        
        Block {
            index,
            timestamp,
            transactions,
            previous_hash,
            hash,
            nonce: 0,
        }
    }
    
    pub fn genesis() -> Self {
        let genesis_transaction = Transaction {
            id: 0,
            from: "Genesis".to_string(),
            to: "Network".to_string(),
            amount: 0.0,
            timestamp: 0,
            shard_id: 0,
        };
        
        Block::new(0, vec![genesis_transaction], "0".to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blockchain {
    pub chain: Vec<Block>,
    pub pending_transactions: Vec<Transaction>,
    pub difficulty: u32,
}

impl Blockchain {
    pub fn new() -> Self {
        Blockchain {
            chain: vec![Block::genesis()],
            pending_transactions: Vec::new(),
            difficulty: 4,
        }
    }
    
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.pending_transactions.push(transaction);
    }
    
    pub fn mine_block(&mut self) -> Option<Block> {
        if self.pending_transactions.is_empty() {
            return None;
        }
        
        let previous_block = self.chain.last().unwrap();
        let new_block = Block::new(
            previous_block.index + 1,
            self.pending_transactions.clone(),
            previous_block.hash.clone(),
        );
        
        self.pending_transactions.clear();
        self.chain.push(new_block.clone());
        Some(new_block)
    }
    
    pub fn get_height(&self) -> u64 {
        self.chain.len() as u64
    }
    
    pub fn get_latest_block(&self) -> &Block {
        self.chain.last().unwrap()
    }
    
    pub fn validate_chain(&self) -> bool {
        for i in 1..self.chain.len() {
            let current = &self.chain[i];
            let previous = &self.chain[i - 1];
            
            if current.previous_hash != previous.hash {
                return false;
            }
        }
        true
    }
}