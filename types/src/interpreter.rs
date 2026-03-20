// fractalchain/evm/src/interpreter.rs
//! EVM interpreter with fractal sharding optimizations
//! Implements optimized instruction execution for parallel processing

use std::collections::HashMap;
use std::convert::TryInto;
use fractalchain_types::Transaction;
use crate::state::EVMState;

/// EVM word size (32 bytes)
const WORD_SIZE: usize = 32;

/// Maximum stack size
const MAX_STACK_SIZE: usize = 1024;

/// Gas costs for instructions
const GAS_ZERO: u64 = 0;
const GAS_BASE: u64 = 2;
const GAS_VERYLOW: u64 = 3;
const GAS_LOW: u64 = 5;
const GAS_MID: u64 = 8;
const GAS_HIGH: u64 = 10;
const GAS_EXTCODE: u64 = 700;
const GAS_BALANCE: u64 = 400;
const GAS_SLOAD: u64 = 200;
const GAS_SSTORE_SET: u64 = 20000;
const GAS_SSTORE_RESET: u64 = 5000;
const GAS_JUMP: u64 = 8;
const GAS_JUMPI: u64 = 10;
const GAS_CREATE: u64 = 32000;
const GAS_CALL: u64 = 700;
const GAS_CALLVALUE: u64 = 9000;
const GAS_CALLSTIPEND: u64 = 2300;

/// EVM execution context
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub address: [u8; 20],
    pub caller: [u8; 20],
    pub value: u128,
    pub gas_limit: u64,
    pub gas_price: u64,
    pub origin: [u8; 20],
    pub block_number: u64,
    pub block_timestamp: u64,
    pub gas_limit_block: u64,
    pub chain_id: u64,
}

impl ExecutionContext {
    pub fn new(tx: &Transaction, block_number: u64, block_timestamp: u64, gas_limit: u64) -> Self {
        Self {
            address: tx.to.unwrap_or([0u8; 20]),
            caller: tx.from,
            value: tx.value,
            gas_limit: tx.gas_limit,
            gas_price: tx.gas_price,
            origin: tx.from,
            block_number,
            block_timestamp,
            gas_limit_block: gas_limit,
            chain_id: 859, // FRACTALCHAIN chain ID
        }
    }
}

/// EVM stack
#[derive(Debug, Clone)]
pub struct Stack {
    data: Vec<[u8; 32]>,
}

impl Stack {
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(MAX_STACK_SIZE),
        }
    }

    pub fn push(&mut self, value: [u8; 32]) -> Result<(), String> {
        if self.data.len() >= MAX_STACK_SIZE {
            return Err("Stack overflow".to_string());
        }
        self.data.push(value);
        Ok(())
    }

    pub fn pop(&mut self) -> Result<[u8; 32], String> {
        self.data.pop().ok_or("Stack underflow".to_string())
    }

    pub fn peek(&self, index: usize) -> Result<[u8; 32], String> {
        let idx = self.data.len().saturating_sub(index + 1);
        self.data.get(idx).copied().ok_or("Invalid stack access".to_string())
    }

    pub fn swap(&mut self, index: usize) -> Result<(), String> {
        if index >= self.data.len() {
            return Err("Invalid swap index".to_string());
        }
        let len = self.data.len();
        let top_idx = len - 1;
        let target_idx = len - index - 1;
        
        self.data.swap(top_idx, target_idx);
        Ok(())
    }

    pub fn dup(&mut self, index: usize) -> Result<(), String> {
        if index == 0 || index > 16 {
            return Err("Invalid dup index".to_string());
        }
        let value = self.peek(index - 1)?;
        self.push(value)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Convert u256 to stack word
    pub fn from_u256(value: &[u8; 32]) -> [u8; 32] {
        *value
    }

    /// Convert stack word to u256
    pub fn to_u256(word: &[u8; 32]) -> [u8; 32] {
        *word
    }

    /// Convert u64 to stack word
    pub fn from_u64(value: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..32].copy_from_slice(&value.to_be_bytes());
        word
    }

    /// Convert stack word to u64
    pub fn to_u64(word: &[u8; 32]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&word[24..32]);
        u64::from_be_bytes(bytes)
    }
}

/// EVM memory
#[derive(Debug, Clone)]
pub struct Memory {
    data: Vec<u8>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
        }
    }

    pub fn read(&self, offset: usize, size: usize) -> Vec<u8> {
        if offset + size > self.data.len() {
            vec![0u8; size]
        } else {
            self.data[offset..offset + size].to_vec()
        }
    }

    pub fn write(&mut self, offset: usize, data: &[u8]) {
        let end = offset + data.len();
        if end > self.data.len() {
            self.data.resize(end, 0u8);
        }
        self.data[offset..end].copy_from_slice(data);
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size, 0u8);
    }

    /// Calculate memory cost
    pub fn calculate_cost(&self, new_size: usize) -> u64 {
        let words = (new_size + 31) / 32;
        words as u64 * GAS_VERYLOW
    }
}

/// EVM interpreter result
#[derive(Debug, Clone)]
pub struct InterpreterResult {
    pub success: bool,
    pub gas_used: u64,
    pub return_data: Vec<u8>,
    pub error: Option<String>,
}

impl InterpreterResult {
    pub fn success(gas_used: u64, return_data: Vec<u8>) -> Self {
        Self {
            success: true,
            gas_used,
            return_data,
            error: None,
        }
    }

    pub fn failure(gas_used: u64, error: String) -> Self {
        Self {
            success: false,
            gas_used,
            return_data: vec![],
            error: Some(error),
        }
    }
}

/// EVM interpreter
#[derive(Debug)]
pub struct Interpreter {
    pub state: *mut EVMState,
    pub context: ExecutionContext,
    pub stack: Stack,
    pub memory: Memory,
    pub pc: usize,
    pub gas_remaining: u64,
    pub return_data: Vec<u8>,
    pub stopped: bool,
}

impl Interpreter {
    pub fn new(state: &mut EVMState, tx: &Transaction) -> Self {
        let context = ExecutionContext::new(
            tx,
            state.get_block_number(),
            state.get_block_timestamp(),
            state.get_gas_limit(),
        );

        Self {
            state: state as *mut EVMState,
            context,
            stack: Stack::new(),
            memory: Memory::new(),
            pc: 0,
            gas_remaining: tx.gas_limit,
            return_data: vec![],
            stopped: false,
        }
    }

    /// Execute transaction
    pub fn execute(&mut self) -> Result<InterpreterResult, String> {
        // Validate initial conditions
        if self.context.gas_limit < 21000 {
            return Ok(InterpreterResult::failure(0, "Insufficient gas".to_string()));
        }

        self.gas_remaining = self.context.gas_limit;
        self.consume_gas(21000)?; // Base transaction cost

        // Execute code if present
        if !self.get_code(&self.context.address).is_empty() {
            self.execute_code()?;
        }

        Ok(InterpreterResult::success(
            self.context.gas_limit - self.gas_remaining,
            self.return_data.clone(),
        ))
    }

    /// Execute contract code
    fn execute_code(&mut self) -> Result<(), String> {
        let code = self.get_code(&self.context.address);
        
        while self.pc < code.len() && !self.stopped {
            let opcode = code[self.pc];
            self.pc += 1;
            
            match opcode {
                0x00 => self.op_stop()?,
                0x01 => self.op_add()?,
                0x02 => self.op_mul()?,
                0x03 => self.op_sub()?,
                0x04 => self.op_div()?,
                0x10 => self.op_lt()?,
                0x11 => self.op_gt()?,
                0x14 => self.op_eq()?,
                0x15 => self.op_iszero()?,
                0x16 => self.op_and()?,
                0x17 => self.op_or()?,
                0x18 => self.op_xor()?,
                0x19 => self.op_not()?,
                0x50 => self.op_pop()?,
                0x51 => self.op_mload()?,
                0x52 => self.op_mstore()?,
                0x53 => self.op_mstore8()?,
                0x54 => self.op_sload()?,
                0x55 => self.op_sstore()?,
                0x56 => self.op_jump()?,
                0x57 => self.op_jumpi()?,
                0x5b => self.op_jumpdest()?,
                0x60..=0x7f => self.op_push(opcode)?,
                0x80..=0x8f => self.op_dup(opcode)?,
                0x90..=0x9f => self.op_swap(opcode)?,
                0xf3 => self.op_return()?,
                0xfd => self.op_revert()?,
                _ => return Err(format!("Unsupported opcode: 0x{:02x}", opcode)),
            }
        }
        
        Ok(())
    }

    /// Get code for address
    fn get_code(&self, address: &[u8; 20]) -> Vec<u8> {
        unsafe {
            (*self.state).get_code(address)
        }
    }

    /// Consume gas
    fn consume_gas(&mut self, amount: u64) -> Result<(), String> {
        if self.gas_remaining < amount {
            return Err("Out of gas".to_string());
        }
        self.gas_remaining -= amount;
        Ok(())
    }

    /// Get current gas price
    fn get_gas_price(&self) -> u64 {
        self.context.gas_price
    }

    /// Get current block number
    fn get_block_number(&self) -> u64 {
        self.context.block_number
    }

    /// Get current block timestamp
    fn get_block_timestamp(&self) -> u64 {
        self.context.block_timestamp
    }

    /// STOP instruction
    fn op_stop(&mut self) -> Result<(), String> {
        self.stopped = true;
        Ok(())
    }

    /// ADD instruction
    fn op_add(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        let result = a_val.overflowing_add(b_val).0;
        
        self.stack.push(bytes_from_u256(&result))?;
        Ok(())
    }

    /// MUL instruction
    fn op_mul(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_LOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        let result = a_val.overflowing_mul(b_val).0;
        
        self.stack.push(bytes_from_u256(&result))?;
        Ok(())
    }

    /// SUB instruction
    fn op_sub(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        let result = a_val.overflowing_sub(b_val).0;
        
        self.stack.push(bytes_from_u256(&result))?;
        Ok(())
    }

    /// DIV instruction
    fn op_div(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_LOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        
        if b_val == 0u32.into() {
            self.stack.push([0u8; 32])?;
        } else {
            let result = a_val / b_val;
            self.stack.push(bytes_from_u256(&result))?;
        }
        
        Ok(())
    }

    /// LT instruction (less than)
    fn op_lt(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        
        let result = if a_val < b_val { 1u32 } else { 0u32 };
        self.stack.push(Stack::from_u64(result as u64))?;
        
        Ok(())
    }

    /// GT instruction (greater than)
    fn op_gt(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let a_val = u256_from_bytes(&a);
        let b_val = u256_from_bytes(&b);
        
        let result = if a_val > b_val { 1u32 } else { 0u32 };
        self.stack.push(Stack::from_u64(result as u64))?;
        
        Ok(())
    }

    /// EQ instruction (equality)
    fn op_eq(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let result = if a == b { 1u32 } else { 0u32 };
        self.stack.push(Stack::from_u64(result as u64))?;
        
        Ok(())
    }

    /// ISZERO instruction
    fn op_iszero(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        
        let result = if a == [0u8; 32] { 1u32 } else { 0u32 };
        self.stack.push(Stack::from_u64(result as u64))?;
        
        Ok(())
    }

    /// AND instruction
    fn op_and(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = a[i] & b[i];
        }
        
        self.stack.push(result)?;
        Ok(())
    }

    /// OR instruction
    fn op_or(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = a[i] | b[i];
        }
        
        self.stack.push(result)?;
        Ok(())
    }

    /// XOR instruction
    fn op_xor(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        let b = self.stack.pop()?;
        
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = a[i] ^ b[i];
        }
        
        self.stack.push(result)?;
        Ok(())
    }

    /// NOT instruction
    fn op_not(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let a = self.stack.pop()?;
        
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = !a[i];
        }
        
        self.stack.push(result)?;
        Ok(())
    }

    /// POP instruction
    fn op_pop(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_BASE)?;
        self.stack.pop()?;
        Ok(())
    }

    /// MLOAD instruction
    fn op_mload(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let offset = self.stack.pop()?;
        let offset_usize = Stack::to_u64(&offset) as usize;
        
        let value = self.memory.read(offset_usize, 32);
        let mut word = [0u8; 32];
        word.copy_from_slice(&value);
        
        self.stack.push(word)?;
        Ok(())
    }

    /// MSTORE instruction
    fn op_mstore(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let offset = self.stack.pop()?;
        let value = self.stack.pop()?;
        
        let offset_usize = Stack::to_u64(&offset) as usize;
        
        self.memory.write(offset_usize, &value);
        Ok(())
    }

    /// MSTORE8 instruction
    fn op_mstore8(&mut self) -> Result<(), String> {
        self.consume_gas(GAS_VERYLOW)?;
        
        let offset = self.stack.pop()?;
        let value = self.stack.pop()?;
        
        let offset_usize = Stack::to_u64(&offset) as usize;
        let
