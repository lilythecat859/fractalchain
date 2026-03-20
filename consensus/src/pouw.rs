// fractalchain/consensus/src/pouw.rs
//! Proof-of-Useful-Work consensus component
//! Provides anti-spam mechanism without premine

use sha2::{Sha256, Digest};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// PoUW difficulty target (adjusts dynamically)
pub const POW_DIFFICULTY_TARGET: u64 = 0x0000ffff00000000;
/// PoUW solution timeout: 5 seconds
pub const POW_TIMEOUT: Duration = Duration::from_secs(5);
/// Useful work factor (must be meaningful computation)
pub const USEFUL_WORK_FACTOR: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoUWSolution {
    /// Block hash this solution is for
    pub block_hash: [u8; 32],
    /// Nonce value
    pub nonce: u64,
    /// Solution difficulty
    pub difficulty: u64,
    /// Useful work proof (Mandelbrot computation)
    pub work_proof: MandelbrotProof,
    /// Solution timestamp
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MandelbrotProof {
    /// Coordinates that were computed
    pub coordinates: Vec<ComplexPoint>,
    /// Iteration counts (proof of work done)
    pub iterations: Vec<u32>,
    /// Hash of computation results
    pub result_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexPoint {
    pub x: f64,
    pub y: f64,
}

pub struct PoUWEngine;

impl PoUWEngine {
    /// Create a new PoUW engine
    pub fn new() -> Self {
        PoUWEngine
    }

    /// Solve PoUW for a block (performs useful Mandelbrot computations)
    pub fn solve(&self, block_hash: [u8; 32], difficulty: u64) -> Option<PoUWSolution> {
        let start_time = SystemTime::now();
        let mut nonce = 0u64;

        loop {
            // Check timeout
            if start_time.elapsed().unwrap() > POW_TIMEOUT {
                return None;
            }

            // Perform useful work: Mandelbrot computations
            let work_proof = self.compute_mandelbrot_work(nonce, USEFUL_WORK_FACTOR);
            
            // Check if solution meets difficulty
            let solution_hash = self.calculate_solution_hash(&block_hash, nonce, &work_proof);
            
            if Self::meets_difficulty(&solution_hash, difficulty) {
                return Some(PoUWSolution {
                    block_hash,
                    nonce,
                    difficulty,
                    work_proof,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                });
            }

            nonce += 1;
        }
    }

    /// Verify PoUW solution
    pub fn verify(&self, solution: &PoUWSolution) -> bool {
        // Verify useful work
        if !self.verify_mandelbrot_work(&solution.work_proof) {
            return false;
        }

        // Verify difficulty
        let solution_hash = self.calculate_solution_hash(
            &solution.block_hash,
            solution.nonce,
            &solution.work_proof,
        );

        Self::meets_difficulty(&solution_hash, solution.difficulty)
    }

    /// Compute Mandelbrot set as useful work
    fn compute_mandelbrot_work(&self, seed: u64, iterations: usize) -> MandelbrotProof {
        let mut coordinates = Vec::new();
        let mut mandelbrot_iterations = Vec::new();
        let mut hasher = Sha256::new();

        for i in 0..iterations {
            // Deterministic coordinate generation from seed
            let x = ((seed.wrapping_add(i as u64) % 1000) as f64 / 100.0) - 2.5;
            let y = (((seed.wrapping_add(i as u64) >> 8) % 1000) as f64 / 100.0) - 1.5;
            
            let c = num_complex::Complex::new(x, y);
            let mut z = num_complex::Complex::new(0.0, 0.0);
            
            let mut iter_count = 0u32;
            while z.norm_sqr() <= 4.0 && iter_count < 1000 {
                z = z * z + c;
                iter_count += 1;
            }

            coordinates.push(ComplexPoint { x, y });
            mandelbrot_iterations.push(iter_count);
            
            // Update hash with computation result
            hasher.update(&iter_count.to_le_bytes());
            hasher.update(&x.to_le_bytes());
            hasher.update(&y.to_le_bytes());
        }

        MandelbrotProof {
            coordinates,
            iterations: mandelbrot_iterations,
            result_hash: hasher.finalize().into(),
        }
    }

    /// Verify Mandelbrot computations
    fn verify_mandelbrot_work(&self, proof: &MandelbrotProof) -> bool {
        if proof.coordinates.len() != proof.iterations.len() {
            return false;
        }

        let mut hasher = Sha256::new();
        
        for (i, coord) in proof.coordinates.iter().enumerate() {
            let c = num_complex::Complex::new(coord.x, coord.y);
            let mut z = num_complex::Complex::new(0.0, 0.0);
            
            let mut iter_count = 0u32;
            while z.norm_sqr() <= 4.0 && iter_count < 1000 {
                z = z * z + c;
                iter_count += 1;
            }

            if iter_count != proof.iterations[i] {
                return false;
            }

            hasher.update(&iter_count.to_le_bytes());
            hasher.update(&coord.x.to_le_bytes());
            hasher.update(&coord.y.to_le_bytes());
        }

        let computed_hash: [u8; 32] = hasher.finalize().into();
        computed_hash == proof.result_hash
    }

    /// Calculate solution hash
    fn calculate_solution_hash(
        &self,
        block_hash: &[u8; 32],
        nonce: u64,
        work_proof: &MandelbrotProof,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(block_hash);
        hasher.update(&nonce.to_le_bytes());
        hasher.update(&work_proof.result_hash);
        hasher.finalize().into()
    }

    /// Check if hash meets difficulty target
    fn meets_difficulty(hash: &[u8; 32], difficulty: u64) -> bool {
        let hash_u64 = u64::from_le_bytes([
            hash[0], hash[1], hash[2], hash[3],
            hash[4], hash[5], hash[6], hash[7],
        ]);
        
        hash_u64 <= difficulty
    }

    /// Adjust difficulty based on solution time
    pub fn adjust_difficulty(current_difficulty: u64, solution_time: Duration) -> u64 {
        let target_time = POW_TIMEOUT.as_secs() / 2;
        let actual_time = solution_time.as_secs();
        
        if actual_time < target_time {
            // Increase difficulty (make harder)
            current_difficulty * 90 / 100
        } else if actual_time > target_time * 2 {
            // Decrease difficulty (make easier)
            current_difficulty * 110 / 100
        } else {
            current_difficulty
        }
    }
}

impl Default for PoUWEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mandelbrot_computation() {
        let engine = PoUWEngine::new();
        let proof = engine.compute_mandelbrot_work(12345, 100);
        
        assert_eq!(proof.coordinates.len(), 100);
        assert_eq!(proof.iterations.len(), 100);
        assert_ne!(proof.result_hash, [0u8; 32]);
    }

    #[test]
    fn test_mandelbrot_verification() {
        let engine = PoUWEngine::new();
        let proof = engine.compute_mandelbrot_work(12345, 50);
        
        assert!(engine.verify_mandelbrot_work(&proof));
        
        // Tamper with proof
        let mut bad_proof = proof.clone();
        bad_proof.iterations[0] = 999999;
        assert!(!engine.verify_mandelbrot_work(&bad_proof));
    }

    #[test]
    fn test_difficulty_adjustment() {
        let current_difficulty = 0x0000ffff00000000;
        
        // Fast solution - increase difficulty
        let fast_time = Duration::from_secs(1);
        let harder_difficulty = PoUWEngine::adjust_difficulty(current_difficulty, fast_time);
        assert!(harder_difficulty < current_difficulty);
        
        // Slow solution - decrease difficulty
        let slow_time = Duration::from_secs(20);
        let easier_difficulty = PoUWEngine::adjust_difficulty(current_difficulty, slow_time);
        assert!(easier_difficulty > current_difficulty);
    }
}