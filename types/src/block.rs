// fractalchain/types/src/block.rs
//! Block structures with fractal sharding support

use serde::{Serialize, Deserialize};
use crate::fractal::FractalShardId;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    pub parent_hash: [u8; 32],
    pub state_root: [u8; 32],
    pub transactions_root: [u8; 32],
    pub receipts_root: [u8; 32],
    pub block_number: u64,
    pub timestamp: u64,
    pub shard_id: FractalShardId,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub difficulty: u64,
    pub nonce: u64,
    pub extra_data: Vec<u8>,
}

impl BlockHeader {
    pub fn new(
        parent_hash: [u8; 32],
        block_number: u64,
        shard_id: FractalShardId,
        gas_limit: u64,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            parent_hash,
            state_root: [0u8; 32],
            transactions_root: [0u8; 32],
            receipts_root: [0u8; 32],
            block_number,
            timestamp,
            shard_id,
            gas_used: 0,
            gas_limit,
            difficulty: Self::calculate_difficulty(shard_id, block_number),
            nonce: 0,
            extra_data: vec![],
        }
    }

    fn calculate_difficulty(shard_id: FractalShardId, block_number: u64) -> u64 {
        // Fractal difficulty adjustment based on shard depth and block number
        let base_difficulty = 1000;
        let depth_factor = 2u64.pow(shard_id.depth());
        let time_factor = block_number / 1000;
        
        base_difficulty * depth_factor + time_factor
    }

    pub fn hash(&self) -> [u8; 32] {
        use blake3::Hasher;
        let mut hasher = Hasher::new();
        
        hasher.update(&self.parent_hash);
        hasher.update(&self.state_root);
        hasher.update(&self.transactions_root);
        hasher.update(&self.block_number.to_be_bytes());
        hasher.update(&self.timestamp.to_be_bytes());
        hasher.update(&self.shard_id.0.to_be_bytes());
        
        *hasher.finalize().as_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<crate::transaction::Transaction>,
}

impl Block {
    pub fn new(header: BlockHeader) -> Self {
        Self {
            header,
            transactions: vec![],
        }
    }

    pub fn add_transaction(&mut self, tx: crate::transaction::Transaction) -> Result<(), String> {
        // Validate transaction belongs to this shard
        let tx_shard = FractalShardId::shard_for_address(&tx.from);
        if tx_shard != self.header.shard_id {
            return Err(format!(
                "Transaction shard mismatch: expected {:?}, got {:?}",
                self.header.shard_id, tx_shard
            ));
        }

        self.transactions.push(tx);
        Ok(())
    }

    pub fn hash(&self) -> [u8; 32] {
        self.header.hash()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let shard = FractalShardId::root();
        let header = BlockHeader::new([0u8; 32], 1, shard, 10000000);
        
        assert_eq!(header.block_number, 1);
        assert_eq!(header.shard_id, shard);
        assert!(header.timestamp > 0);
    }

    #[test]
    fn test_difficulty_calculation() {
        let shard = FractalShardId(0x1000000000000000); // depth 1
        let difficulty = BlockHeader::calculate_difficulty(shard, 1000);
        
        assert_eq!(difficulty, 2000); // 1000 * 2^1
    }
}
