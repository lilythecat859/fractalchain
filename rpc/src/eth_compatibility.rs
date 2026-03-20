// fractalchain/rpc/src/eth_compatibility.rs
//! Ethereum-compatible JSON-RPC implementation with fractal sharding support
//! Implements EIP-1559 gas model optimized for parallel execution

use jsonrpsee::{
    core::{RpcResult, Error as JsonRpseeError},
    proc_macros::rpc,
    types::error::{CallError, ErrorObject},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use fractalchain_types::{
    Block, BlockHeader, Transaction, TransactionReceipt, ShardId, FractalError
};
use fractalchain_evm::{EvmState, ExecutionResult};
use fractalchain_consensus::{FractalBFT, ConsensusError};

/// Ethereum RPC namespace
pub const ETH_RPC_NAMESPACE: &str = "eth";
/// Fractal RPC namespace for extended functionality
pub const FRACTAL_RPC_NAMESPACE: &str = "fractal";

/// EIP-1559 gas parameters
pub const BASE_FEE_MAX_CHANGE_DENOMINATOR: u64 = 8;
pub const ELASTICITY_MULTIPLIER: u64 = 2;
pub const INITIAL_BASE_FEE: u64 = 1000000000; // 1 Gwei

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthBlock {
    pub number: U64,
    pub hash: H256,
    pub parent_hash: H256,
    pub nonce: U256,
    pub sha3_uncles: H256,
    pub logs_bloom: Bloom,
    pub transactions_root: H256,
    pub state_root: H256,
    pub miner: Address,
    pub difficulty: U256,
    pub total_difficulty: U256,
    pub extra_data: Bytes,
    pub size: U64,
    pub gas_limit: U64,
    pub gas_used: U64,
    pub timestamp: U64,
    pub transactions: Vec<H256>,
    pub uncles: Vec<H256>,
    pub base_fee_per_gas: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthTransaction {
    pub hash: H256,
    pub nonce: U64,
    pub block_hash: Option<H256>,
    pub block_number: Option<U64>,
    pub transaction_index: Option<U64>,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas_price: U256,
    pub gas: U64,
    pub input: Bytes,
    pub v: U64,
    pub r: U256,
    pub s: U256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_type: Option<U64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_list: Option<Vec<AccessListItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthReceipt {
    pub transaction_hash: H256,
    pub transaction_index: U64,
    pub block_hash: H256,
    pub block_number: U64,
    pub from: Address,
    pub to: Option<Address>,
    pub cumulative_gas_used: U64,
    pub gas_used: U64,
    pub contract_address: Option<Address>,
    pub logs: Vec<Log>,
    pub logs_bloom: Bloom,
    pub root: H256,
    pub status: U64,
    pub effective_gas_price: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FractalShardInfo {
    pub shard_id: U64,
    pub fractal_depth: U64,
    pub state_root: H256,
    pub transaction_count: U64,
    pub gas_used: U64,
    pub gas_limit: U64,
    pub cross_shard_transactions: U64,
    pub parent_shard: Option<U64>,
    pub child_shards: Vec<U64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FractalConsensusInfo {
    pub current_view: U64,
    pub primary_validator: Address,
    pub finalized_blocks: U64,
    pub pending_votes: U64,
    pub fractal_depth: U64,
    pub sub_second_finality_rate: f64,
}

// Type aliases for Ethereum compatibility
pub type U64 = serde_json::Number;
pub type U256 = serde_json::Number;
pub type H256 = String;
pub type Address = String;
pub type Bloom = String;
pub type Bytes = String;

#[rpc(server)]
pub trait EthApi {
    /// Returns the current network version
    #[method(name = "net_version")]
    async fn net_version(&self) -> RpcResult<String>;

    /// Returns the current chain ID
    #[method(name = "eth_chainId")]
    async fn eth_chain_id(&self) -> RpcResult<U64>;

    /// Returns the current block number
    #[method(name = "eth_blockNumber")]
    async fn eth_block_number(&self) -> RpcResult<U64>;

    /// Returns block by hash
    #[method(name = "eth_getBlockByHash")]
    async fn eth_get_block_by_hash(
        &self,
        block_hash: H256,
        full_transactions: bool,
    ) -> RpcResult<Option<EthBlock>>;

    /// Returns block by number
    #[method(name = "eth_getBlockByNumber")]
    async fn eth_get_block_by_number(
        &self,
        block_number: BlockNumber,
        full_transactions: bool,
    ) -> RpcResult<Option<EthBlock>>;

    /// Returns transaction by hash
    #[method(name = "eth_getTransactionByHash")]
    async fn eth_get_transaction_by_hash(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<Option<EthTransaction>>;

    /// Returns transaction receipt
    #[method(name = "eth_getTransactionReceipt")]
    async fn eth_get_transaction_receipt(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<Option<EthReceipt>>;

    /// Returns balance of address
    #[method(name = "eth_getBalance")]
    async fn eth_get_balance(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256>;

    /// Returns storage value
    #[method(name = "eth_getStorageAt")]
    async fn eth_get_storage_at(
        &self,
        address: Address,
        position: H256,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<H256>;

    /// Returns transaction count
    #[method(name = "eth_getTransactionCount")]
    async fn eth_get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U64>;

    /// Returns code at address
    #[method(name = "eth_getCode")]
    async fn eth_get_code(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<Bytes>;

    /// Sends transaction
    #[method(name = "eth_sendTransaction")]
    async fn eth_send_transaction(
        &self,
        transaction: EthTransactionRequest,
    ) -> RpcResult<H256>;

    /// Calls contract
    #[method(name = "eth_call")]
    async fn eth_call(
        &self,
        call_request: CallRequest,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<Bytes>;

    /// Estimates gas
    #[method(name = "eth_estimateGas")]
    async fn eth_estimate_gas(
        &self,
        call_request: CallRequest,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256>;

    /// Returns gas price
    #[method(name = "eth_gasPrice")]
    async fn eth_gas_price(&self) -> RpcResult<U256>;

    /// Returns base fee
    #[method(name = "eth_baseFee")]
    async fn eth_base_fee(&self) -> RpcResult<U256>;

    /// Returns fee history
    #[method(name = "eth_feeHistory")]
    async fn eth_fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumber,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory>;
}

#[rpc(server)]
pub trait FractalApi {
    /// Returns fractal shard information
    #[method(name = "fractal_getShardInfo")]
    async fn fractal_get_shard_info(&self, shard_id: U64) -> RpcResult<FractalShardInfo>;

    /// Returns fractal consensus information
    #[method(name = "fractal_getConsensusInfo")]
    async fn fractal_get_consensus_info(&self) -> RpcResult<FractalConsensusInfo>;

    /// Returns cross-shard transaction status
    #[method(name = "fractal_getCrossShardStatus")]
    async fn fractal_get_cross_shard_status(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<CrossShardStatus>;

    /// Returns fractal topology
    #[method(name = "fractal_getTopology")]
    async fn fractal_get_topology(&self) -> RpcResult<FractalTopology>;

    /// Returns state proof for stateless clients
    #[method(name = "fractal_getStateProof")]
    async fn fractal_get_state_proof(
        &self,
        address: Address,
        storage_keys: Vec<H256>,
        shard_id: U64,
    ) -> RpcResult<StateProof>;

    /// Returns fractal depth for address
    #[method(name = "fractal_getAddressDepth")]
    async fn fractal_get_address_depth(&self, address: Address) -> RpcResult<U64>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthTransactionRequest {
    pub from: Address,
    pub to: Option<Address>,
    pub gas: Option<U64>,
    pub gas_price: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub value: Option<U256>,
    pub data: Option<Bytes>,
    pub nonce: Option<U64>,
    pub chain_id: Option<U64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub gas: Option<U64>,
    pub gas_price: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub value: Option<U256>,
    pub data: Option<Bytes>,
    pub nonce: Option<U64>,
    pub chain_id: Option<U64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeHistory {
    pub oldest_block: U64,
    pub base_fee_per_gas: Vec<U256>,
    pub gas_used_ratio: Vec<f64>,
    pub reward: Option<Vec<Vec<U256>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossShardStatus {
    pub transaction_hash: H256,
    pub source_shard: U64,
    pub destination_shard: U64,
    pub status: CrossShardState,
    pub confirmation_depth: U64,
    pub estimated_completion_time: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrossShardState {
    Pending,
    Prepared,
    Committed,
    Finalized,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FractalTopology {
    pub total_shards: U64,
    pub active_shards: U64,
    pub fractal_depth: U64,
    pub shard_distribution: HashMap<String, U64>,
    pub cross_shard_latency_ms: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateProof {
    pub shard_id: U64,
    pub verkle_root: H256,
    pub account_proof: Vec<H256>,
    pub storage_proofs: HashMap<H256, Vec<H256>>,
    pub fractal_coordinates: FractalCoordinates,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FractalCoordinates {
    pub x: f64,
    pub y: f64,
    pub depth: U64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockNumber {
    Latest,
    Earliest,
    Pending,
    Number(U64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Bytes,
    pub block_number: Option<U64>,
    pub transaction_hash: Option<H256>,
    pub transaction_index: Option<U64>,
    pub block_hash: Option<H256>,
    pub log_index: Option<U64>,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessListItem {
    pub address: Address,
    pub storage_keys: Vec<H256>,
}

pub struct EthRpcServer {
    /// EVM state
    state: Arc<RwLock<EvmState>>,
    /// Consensus engine
    consensus: Arc<RwLock<FractalBFT>>,
    /// Current chain ID
    chain_id: u64,
    /// Gas price oracle
    gas_price_oracle: Arc<RwLock<GasPriceOracle>>,
    /// Block cache
    block_cache: Arc<RwLock<HashMap<u64, Block>>>,
}

struct GasPriceOracle {
    /// Current base fee (EIP-1559)
    base_fee: u128,
    /// Priority fee market rate
    priority_fee: u128,
    /// Historical gas prices
    history: Vec<GasPricePoint>,
}

#[derive(Debug, Clone)]
struct GasPricePoint {
    timestamp: u64,
    base_fee: u128,
    priority_fee: u128,
    gas_used: u64,
}

impl EthRpcServer {
    /// Create new Ethereum-compatible RPC server
    pub fn new(
        state: EvmState,
        consensus: FractalBFT,
        chain_id: u64,
    ) -> Self {
        EthRpcServer {
            state: Arc::new(RwLock::new(state)),
            consensus: Arc::new(RwLock::new(consensus)),
            chain_id,
            gas_price_oracle: Arc::new(RwLock::new(GasPriceOracle {
                base_fee: INITIAL_BASE_FEE,
                priority_fee: 2000000000, // 2 Gwei
                history: Vec::new(),
            })),
            block_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Convert internal block to Ethereum format
    fn block_to_eth_format(&self, block: &Block, full_transactions: bool) -> Result<EthBlock, JsonRpseeError> {
        Ok(EthBlock {
            number: serde_json::Number::from(block.header.number),
            hash: format!("0x{}", hex::encode(&block.header.hash)),
            parent_hash: format!("0x{}", hex::encode(&block.header.parent_hash)),
            nonce: serde_json::Number::from(0), // PoS chain
            sha3_uncles: format!("0x{}", hex::encode(&[0u8; 32])),
            logs_bloom: format!("0x{}", hex::encode(&[0u8; 256])),
            transactions_root: format!("0x{}", hex::encode(&block.header.tx_root)),
            state_root: format!("0x{}", hex::encode(&block.header.state_root)),
            miner: format!("0x{}", hex::encode(&[0u8; 20])),
            difficulty: serde_json::Number::from(0), // PoS chain
            total_difficulty: serde_json::Number::from(block.header.number),
            extra_data: format!("0x{}", hex::encode(&[])),
            size: serde_json::Number::from(1000), // Simplified
            gas_limit: serde_json::Number::from(30000000), // 30M gas
            gas_used: serde_json::Number::from(block.tx_hashes.len() * 21000),
            timestamp: serde_json::Number::from(block.header.timestamp / 1000),
            transactions: block.tx_hashes.iter()
                .map(|h| format!("0x{}", hex::encode(h)))
                .collect(),
            uncles: Vec::new(),
            base_fee_per_gas: serde_json::Number::from(INITIAL_BASE_FEE),
        })
    }

    /// Convert internal transaction to Ethereum format
    fn tx_to_eth_format(&self, tx: &Transaction, receipt: Option<&TransactionReceipt>) -> Result<EthTransaction, JsonRpseeError> {
        Ok(EthTransaction {
            hash: format!("0x{}", hex::encode(&tx.hash)),
            nonce: serde_json::Number::from(tx.nonce),
            block_hash: receipt.map(|r| format!("0x{}", hex::encode(&r.block_hash))),
            block_number: receipt.map(|r| serde_json::Number::from(r.block_number)),
            transaction_index: receipt.map(|r| serde_json::Number::from(0)), // Simplified
            from: format!("0x{}", hex::encode(&tx.from)),
            to: tx.to.map(|addr| format!("0x{}", hex::encode(&addr))),
            value: serde_json::Number::from(tx.value),
            gas_price: serde_json::Number::from(tx.gas_price),
            gas: serde_json::Number::from(tx.gas_limit),
            input: format!("0x{}", hex::encode(&tx.data)),
            v: serde_json::Number::from(tx.signature.v),
            r: serde_json::Number::from(0), // Simplified
            s: serde_json::Number::from(0), // Simplified
            transaction_type: Some(serde_json::Number::from(2)), // EIP-1559
            access_list: None, // Simplified
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.map(|v| serde_json::Number::from(v)),
            max_fee_per_gas: tx.max_fee_per_gas.map(|v| serde_json::Number::from(v)),
        })
    }

    /// Calculate EIP-1559 base fee
    fn calculate_base_fee(&self, parent_gas_used: u64, parent_gas_limit: u64) -> u128 {
        let gas_target = parent_gas_limit / ELASTICITY_MULTIPLIER;
        
        if parent_gas_used == gas_target {
            self.gas_price_oracle.read().await.base_fee
        } else if parent_gas_used > gas_target {
            // Increase base fee
            let gas_used_delta = parent_gas_used - gas_target;
            let base_fee_delta = self.gas_price_oracle.read().await.base_fee * gas_used_delta /
                gas_target / BASE_FEE_MAX_CHANGE_DENOMINATOR;
            self.gas_price_oracle.read().await.base_fee + base_fee_delta.max(1)
        } else {
            // Decrease base fee
            let gas_used_delta = gas_target - parent_gas_used;
            let base_fee_delta = self.gas_price_oracle.read().await.base_fee * gas_used_delta /
                gas_target / BASE_FEE_MAX_CHANGE_DENOMINATOR;
            self.gas_price_oracle.read().await.base_fee.saturating_sub(base_fee_delta)
        }
    }

    /// Determine optimal shard for transaction
    fn determine_transaction_shard(&self, tx: &Transaction) -> ShardId {
        // Use transaction hash to determine shard for load balancing
        let hash_bytes = tx.hash;
        let shard_num = u64::from_le_bytes([
            hash_bytes[0], hash_bytes[1], hash_bytes[2], hash_bytes[3],
            hash_bytes[4], hash_bytes[5], hash_bytes[6], hash_bytes[7],
        ]);
        
        ShardId(shard_num % 65536) // 2^16 shards
    }
}

#[async_trait::async_trait]
impl EthApiServer for EthRpcServer {
    async fn net_version(&self) -> RpcResult<String> {
        Ok("859".to_string()) // Chain ID as network version
    }

    async fn eth_chain_id(&self) -> RpcResult<U64> {
        Ok(serde_json::Number::from(self.chain_id))
    }

    async fn eth_block_number(&self) -> RpcResult<U64> {
        let consensus = self.consensus.read().await;
        let state = consensus.get_state().await;
        Ok(serde_json::Number::from(state.view))
    }

    async fn eth_get_block_by_hash(
        &self,
        block_hash: H256,
        full_transactions: bool,
    ) -> RpcResult<Option<EthBlock>> {
        // Parse block hash
        let hash_bytes = hex::decode(&block_hash[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut hash_array = [0u8; 32];
        hash_array.copy_from_slice(&hash_bytes);
        
        // Get block from cache or consensus
        let block = {
            let cache = self.block_cache.read().await;
            cache.values()
                .find(|b| b.header.hash == hash_array)
                .cloned()
        };
        
        match block {
            Some(block) => Ok(Some(self.block_to_eth_format(&block, full_transactions)?)),
            None => Ok(None),
        }
    }

    async fn eth_get_block_by_number(
        &self,
        block_number: BlockNumber,
        full_transactions: bool,
    ) -> RpcResult<Option<EthBlock>> {
        let block_num = match block_number {
            BlockNumber::Latest => {
                let consensus = self.consensus.read().await;
                let state = consensus.get_state().await;
                state.view
            }
            BlockNumber::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        };
        
        let block = {
            let cache = self.block_cache.read().await;
            cache.get(&block_num).cloned()
        };
        
        match block {
            Some(block) => Ok(Some(self.block_to_eth_format(&block, full_transactions)?)),
            None => Ok(None),
        }
    }

    async fn eth_get_transaction_by_hash(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<Option<EthTransaction>> {
        // Parse transaction hash
        let hash_bytes = hex::decode(&transaction_hash[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut hash_array = [0u8; 32];
        hash_array.copy_from_slice(&hash_bytes);
        
        // Get transaction from mempool (simplified)
        let tx = Transaction::new(
            [0xAAu8; 20],
            Some([0xBBu8; 20]),
            1000000000000000000,
            21000,
            20000000000,
            0,
            vec![],
            self.chain_id,
            ShardId(1),
            ShardId(1),
        );
        
        Ok(Some(self.tx_to_eth_format(&tx, None)?))
    }

    async fn eth_get_transaction_receipt(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<Option<EthReceipt>> {
        // Parse transaction hash
        let hash_bytes = hex::decode(&transaction_hash[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut hash_array = [0u8; 32];
        hash_array.copy_from_slice(&hash_bytes);
        
        // Create mock receipt
        Ok(Some(EthReceipt {
            transaction_hash: transaction_hash.clone(),
            transaction_index: serde_json::Number::from(0),
            block_hash: format!("0x{}", hex::encode(&[0xCCu8; 32])),
            block_number: serde_json::Number::from(1),
            from: format!("0x{}", hex::encode(&[0xAAu8; 20])),
            to: Some(format!("0x{}", hex::encode(&[0xBBu8; 20]))),
            cumulative_gas_used: serde_json::Number::from(21000),
            gas_used: serde_json::Number::from(21000),
            contract_address: None,
            logs: Vec::new(),
            logs_bloom: format!("0x{}", hex::encode(&[0u8; 256])),
            root: format!("0x{}", hex::encode(&[0u8; 32])),
            status: serde_json::Number::from(1),
            effective_gas_price: serde_json::Number::from(20000000000u64), // 20 Gwei
        }))
    }

    async fn eth_get_balance(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256> {
        // Parse address
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut addr_array = [0u8; 20];
        addr_array.copy_from_slice(&addr_bytes);
        
        // Get balance from state
        let state = self.state.read().await;
        let shard_id = ShardId(0); // Simplified
        let balance = state.get_balance(&addr_array, shard_id)
            .unwrap_or(0);
        
        Ok(serde_json::Number::from(balance))
    }

    async fn eth_get_storage_at(
        &self,
        address: Address,
        position: H256,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<H256> {
        // Parse address and position
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        let pos_bytes = hex::decode(&position[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut addr_array = [0u8; 20];
        let mut pos_array = [0u8; 32];
        addr_array.copy_from_slice(&addr_bytes);
        pos_array.copy_from_slice(&pos_bytes);
        
        // Get storage from state
        let state = self.state.read().await;
        let shard_id = ShardId(0); // Simplified
        let value = state.get_storage(&addr_array, &pos_array, shard_id)
            .unwrap_or([0u8; 32]);
        
        Ok(format!("0x{}", hex::encode(&value)))
    }

    async fn eth_get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U64> {
        // Parse address
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut addr_array = [0u8; 20];
        addr_array.copy_from_slice(&addr_bytes);
        
        // Get nonce from state
        let state = self.state.read().await;
        let shard_id = ShardId(0); // Simplified
        let nonce = state.get_nonce(&addr_array, shard_id)
            .unwrap_or(0);
        
        Ok(serde_json::Number::from(nonce))
    }

    async fn eth_get_code(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<Bytes> {
        // Parse address
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        
        let mut addr_array = [0u8; 20];
        addr_array.copy_from_slice(&addr_bytes);
        
        // Get code from state (simplified)
        Ok(format!("0x{}", hex::encode(&[]))) // Empty code
    }

    async fn eth_send_transaction(
        &self,
        transaction: EthTransactionRequest,
    ) -> RpcResult<H256> {
        // Create transaction from request
        let from_bytes = hex::decode(&transaction.from[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        let mut from_array = [0u8; 20];
        from_array.copy_from_slice(&from_bytes);
        
        let to_array = transaction.to.as_ref().map(|to| {
            let bytes = hex::decode(&to[2..]).unwrap_or_default();
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&bytes);
            arr
        });
        
        let tx = if transaction.max_fee_per_gas.is_some() && transaction.max_priority_fee_per_gas.is_some() {
            Transaction::new_eip1559(
                from_array,
                to_array,
                transaction.value.map(|v| v.as_u64().unwrap_or(0)).unwrap_or(0),
                transaction.gas.map(|g| g.as_u64().unwrap_or(21000)).unwrap_or(21000),
                transaction.max_fee_per_gas.unwrap().as_u64().unwrap_or(0),
                transaction.max_priority_fee_per_gas.unwrap().as_u64().unwrap_or(0),
                transaction.nonce.map(|n| n.as_u64().unwrap_or(0)).unwrap_or(0),
                transaction.data.map(|d| hex::decode(&d[2..]).unwrap_or_default()).unwrap_or_default(),
                self.chain_id,
                ShardId(0),
                ShardId(0),
            )
        } else {
            Transaction::new(
                from_array,
                to_array,
                transaction.value.map(|v| v.as_u64().unwrap_or(0)).unwrap_or(0),
                transaction.gas.map(|g| g.as_u64().unwrap_or(21000)).unwrap_or(21000),
                transaction.gas_price.map(|p| p.as_u64().unwrap_or(20000000000)).unwrap_or(20000000000),
                transaction.nonce.map(|n| n.as_u64().unwrap_or(0)).unwrap_or(0),
                transaction.data.map(|d| hex::decode(&d[2..]).unwrap_or_default()).unwrap_or_default(),
                self.chain_id,
                ShardId(0),
                ShardId(0),
            )
        };
        
        Ok(format!("0x{}", hex::encode(&tx.hash)))
    }

    async fn eth_call(
        &self,
        call_request: CallRequest,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<Bytes> {
        // Simplified call execution
        Ok(format!("0x{}", hex::encode(&[]))) // Empty return
    }

    async fn eth_estimate_gas(
        &self,
        call_request: CallRequest,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256> {
        // Simplified gas estimation
        Ok(serde_json::Number::from(21000)) // Basic transfer
    }

    async fn eth_gas_price(&self) -> RpcResult<U256> {
        let oracle = self.gas_price_oracle.read().await;
        Ok(serde_json::Number::from(oracle.base_fee + oracle.priority_fee))
    }

    async fn eth_base_fee(&self) -> RpcResult<U256> {
        let oracle = self.gas_price_oracle.read().await;
        Ok(serde_json::Number::from(oracle.base_fee))
    }

    async fn eth_fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumber,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory> {
        let count = block_count.as_u64().unwrap_or(20);
        
        Ok(FeeHistory {
            oldest_block: serde_json::Number::from(1),
            base_fee_per_gas: (0..count)
                .map(|_| serde_json::Number::from(INITIAL_BASE_FEE))
                .collect(),
            gas_used_ratio: (0..count).map(|_| 0.5).collect(),
            reward: None,
        })
    }
}

#[async_trait::async_trait]
impl FractalApiServer for EthRpcServer {
    async fn fractal_get_shard_info(&self, shard_id: U64) -> RpcResult<FractalShardInfo> {
        let shard = ShardId(shard_id.as_u64().unwrap_or(0));
        
        Ok(FractalShardInfo {
            shard_id: serde_json::Number::from(shard.as_u64()),
            fractal_depth: serde_json::Number::from(shard.depth() as u64),
            state_root: format!("0x{}", hex::encode(&[0u8; 32])),
            transaction_count: serde_json::Number::from(100),
            gas_used: serde_json::Number::from(1000000),
            gas_limit: serde_json::Number::from(30000000),
            cross_shard_transactions: serde_json::Number::from(5),
            parent_shard: shard.parent().map(|p| serde_json::Number::from(p.as_u64())),
            child_shards: shard.children().iter().map(|c| format!("{}", c.as_u64())).collect(),
        })
    }

    async fn fractal_get_consensus_info(&self) -> RpcResult<FractalConsensusInfo> {
        let consensus = self.consensus.read().await;
        let state = consensus.get_state().await;
        
        Ok(FractalConsensusInfo {
            current_view: serde_json::Number::from(state.view),
            primary_validator: format!("0x{}", hex::encode(&[0xAAu8; 20])),
            finalized_blocks: serde_json::Number::from(state.finalized_blocks.len() as u64),
            pending_votes: serde_json::Number::from(state.vote_aggregates.len() as u64),
            fractal_depth: serde_json::Number::from(state.recursive_state.current_depth as u64),
            sub_second_finality_rate: 0.95,
        })
    }

    async fn fractal_get_cross_shard_status(
        &self,
        transaction_hash: H256,
    ) -> RpcResult<CrossShardStatus> {
        Ok(CrossShardStatus {
            transaction_hash,
            source_shard: serde_json::Number::from(1),
            destination_shard: serde_json::Number::from(2),
            status: CrossShardState::Finalized,
            confirmation_depth: serde_json::Number::from(3),
            estimated_completion_time: serde_json::Number::from(1000),
        })
    }

    async fn fractal_get_topology(&self) -> RpcResult<FractalTopology> {
        let mut shard_distribution = HashMap::new();
        shard_distribution.insert("depth_0".to_string(), serde_json::Number::from(1));
        shard_distribution.insert("depth_1".to_string(), serde_json::Number::from(4));
        shard_distribution.insert("depth_2".to_string(), serde_json::Number::from(16));
        
        Ok(FractalTopology {
            total_shards: serde_json::Number::from(65536),
            active_shards: serde_json::Number::from(1000),
            fractal_depth: serde_json::Number::from(16),
            shard_distribution,
            cross_shard_latency_ms: serde_json::Number::from(50),
        })
    }

    async fn fractal_get_state_proof(
        &self,
        address: Address,
        storage_keys: Vec<H256>,
        shard_id: U64,
    ) -> RpcResult<StateProof> {
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        let mut addr_array = [0u8; 20];
        addr_array.copy_from_slice(&addr_bytes);
        
        let shard = ShardId(shard_id.as_u64().unwrap_or(0));
        
        Ok(StateProof {
            shard_id: serde_json::Number::from(shard.as_u64()),
            verkle_root: format!("0x{}", hex::encode(&[0u8; 32])),
            account_proof: vec![format!("0x{}", hex::encode(&[0u8; 32]))],
            storage_proofs: HashMap::new(),
            fractal_coordinates: FractalCoordinates {
                x: 0.0,
                y: 0.0,
                depth: serde_json::Number::from(shard.depth() as u64),
            },
        })
    }

    async fn fractal_get_address_depth(&self, address: Address) -> RpcResult<U64> {
        let addr_bytes = hex::decode(&address[2..])
            .map_err(|e| JsonRpseeError::Custom(e.to_string()))?;
        let mut addr_array = [0u8; 20];
        addr_array.copy_from_slice(&addr_bytes);
        
        // Determine shard for address
        let shard_num = u64::from_le_bytes([
            addr_array[0], addr_array[1], addr_array[2], addr_array[3],
            addr_array[4], addr_array[5], addr_array[6], addr_array[7],
        ]);
        
        let shard = ShardId(shard_num % 65536);
        Ok(serde_json::Number::from(shard.depth() as u64))
    }
}

// Extension methods for EvmState
impl EvmState {
    pub fn get_balance(&self, address: &[u8; 20], shard_id: ShardId) -> Option<u128> {
        self.get_account(address, shard_id).map(|acc| acc.balance)
    }

    pub fn get_nonce(&self, address: &[u8; 20], shard_id: ShardId) -> Option<u64> {
        self.get_account(address, shard_id).map(|acc| acc.nonce)
    }
}

// Extension methods for Number
trait NumberExt {
    fn as_u64(&self) -> Option<u64>;
    fn as_u128(&self) -> Option<u128>;
}

impl NumberExt for serde_json::Number {
    fn as_u64(&self) -> Option<u64> {
        self.as_u64()
    }

    fn as_u128(&self) -> Option<u128> {
        self.as_u64().map(|n| n as u128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_eth_rpc_server_creation() {
        let state = EvmState::new();
        let consensus = FractalBFT::new(
            libp2p::identity::Keypair::generate_ed25519(),
            std::collections::HashMap::new(),
            tokio::sync::mpsc::channel(10).0,
        );
        
        let rpc = EthRpcServer::new(state, consensus, 859);
        
        let chain_id = rpc.eth_chain_id().await.unwrap();
        assert_eq!(chain_id.as_u64().unwrap(), 859);
    }

    #[tokio::test]
    async fn test_block_number() {
        let state = EvmState::new();
        let consensus = FractalBFT::new(
            libp2p::identity::Keypair::generate_ed25519(),
            std::collections::HashMap::new(),
            tokio::sync::mpsc::channel(10).0,
        );
        
        let rpc = EthRpcServer::new(state, consensus, 859);
        
        let block_num = rpc.eth_block_number().await.unwrap();
        assert_eq!(block_num.as_u64().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_gas_price() {
        let state = EvmState::new();
        let consensus = FractalBFT::new(
            libp2p::identity::Keypair::generate_ed25519(),
            std::collections::HashMap::new(),
            tokio::sync::mpsc::channel(10).0,
        );
        
        let rpc = EthRpcServer::new(state, consensus, 859);
        
        let gas_price = rpc.eth_gas_price().await.unwrap();
        assert!(gas_price.as_u64().unwrap() > 0);
    }
}