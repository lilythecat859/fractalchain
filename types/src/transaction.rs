// fractalchain/types/src/transaction.rs
//! Transaction types with fractal sharding

use serde::{Serialize, Deserialize};
use crate::fractal::FractalShardId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub nonce: u64,
    pub gas_price: u64,
    pub gas_limit: u64,
    pub to: Option<[u8; 20]>,
    pub value: u128,
    pub data: Vec<u8>,
    pub from: [u8; 20],
    pub signature: Signature,
    pub shard_hint: Option<FractalShardId>, // Optional hint for routing
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub v: u64,
    pub r: [u8; 32],
    pub s: [u8; 32],
}

impl Transaction {
    pub fn new(
        nonce: u64,
        gas_price: u64,
        gas_limit: u64,
        to: Option<[u8; 20]>,
        value: u128,
        data: Vec<u8>,
        from: [u8; 20],
    ) -> Self {
        Self {
            nonce,
            gas_price,
            gas_limit,
            to,
            value,
            data,
            from,
            signature: Signature { v: 0, r: [0u8; 32], s: [0u8; 32] },
            shard_hint: None,
        }
    }

    /// Determine which shard should process this transaction
    pub fn target_shard(&self) -> FractalShardId {
        if let Some(hint) = self.shard_hint {
            return hint;
        }

        // Use sender address for shard determination
        FractalShardId::shard_for_address(&self.from)
    }

    /// Calculate transaction hash
    pub fn hash(&self) -> [u8; 32] {
        use blake3::Hasher;
        let mut hasher = Hasher::new();
        
        hasher.update(&self.nonce.to_be_bytes());
        hasher.update(&self.gas_price.to_be_bytes());
        hasher.update(&self.gas_limit.to_be_bytes());
        
        if let Some(to) = &self.to {
            hasher.update(to);
        }
        
        hasher.update(&self.value.to_be_bytes());
        hasher.update(&self.data);
        hasher.update(&self.from);
        
        *hasher.finalize().as_bytes()
    }

    /// Verify basic transaction validity
    pub fn validate(&self) -> Result<(), String> {
        if self.gas_limit < 21000 {
            return Err("Gas limit below minimum".to_string());
        }

        if self.gas_price == 0 {
            return Err("Gas price cannot be zero".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let from = [0x42u8; 20];
        let tx = Transaction::new(
            1,
            2000000000, // 2 Gwei
            21000,
            Some([0x43u8; 20]),
            1000000000000000000, // 1 ETH
            vec![],
            from,
        );

        assert_eq!(tx.nonce, 1);
        assert_eq!(tx.gas_price, 2000000000);
    }

    #[test]
    fn test_shard_determination() {
        let from = [0x42u8; 20];
        let tx = Transaction::new(
            1,
            2000000000,
            21000,
            None,
            0,
            vec![],
            from,
        );

        let shard = tx.target_shard();
        assert!(shard.depth() <= crate::fractal::MAX_FRACTAL_DEPTH);
    }
}
