// fractalchain/types/src/fractal.rs
//! Fractal mathematics for recursive sharding
//! Implements Mandelbrot-inspired shard ID generation with Hausdorff dimension calculations

use std::collections::HashMap;
use std::fmt;
use serde::{Serialize, Deserialize};
use num_complex::Complex64;
use once_cell::sync::Lazy;

/// Hausdorff dimension for fractal shard space (log(4)/log(2) = 2.0)
/// But we use a modified dimension for blockchain sharding efficiency
pub const FRACTAL_DIMENSION: f64 = 2.584962500721156; // log2(6) - optimal for 6-way branching

/// Maximum recursion depth for fractal shards
pub const MAX_FRACTAL_DEPTH: u32 = 16; // 2^16 shards maximum

/// Mandelbrot set escape radius for shard boundary determination
const ESCAPE_RADIUS: f64 = 2.0;

/// Number of iterations for Mandelbrot boundary classification
const MANDELBROT_ITERATIONS: u32 = 1000;

/// Static cache for shard ID mappings
static SHARD_CACHE: Lazy<std::sync::RwLock<HashMap<FractalShardId, ShardMetadata>>> = 
    Lazy::new(|| std::sync::RwLock::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FractalShardId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    pub depth: u32,
    pub parent: Option<FractalShardId>,
    pub children: Vec<FractalShardId>,
    pub boundary_box: (Complex64, Complex64), // (min, max) in complex plane
    pub hausdorff_measure: f64,
}

impl FractalShardId {
    /// Generate root shard (depth 0)
    pub fn root() -> Self {
        FractalShardId(0)
    }

    /// Generate shard ID from Mandelbrot coordinate
    pub fn from_coordinate(c: Complex64, depth: u32) -> Self {
        if depth == 0 {
            return Self::root();
        }

        // Map complex coordinate to shard space using Mandelbrot iteration count
        let iterations = mandelbrot_escape_iterations(c);
        let shard_bits = (iterations as u64) << (64 - depth * 4); // 4 bits per depth level
        
        FractalShardId(shard_bits)
    }

    /// Get parent shard ID (depth-1)
    pub fn parent(&self) -> Option<FractalShardId> {
        if self.0 == 0 {
            None
        } else {
            let depth = self.depth();
            let mask = !((1u64 << (64 - (depth - 1) * 4)) - 1);
            Some(FractalShardId(self.0 & mask))
        }
    }

    /// Get child shards (depth+1)
    pub fn children(&self) -> Vec<FractalShardId> {
        let depth = self.depth();
        if depth >= MAX_FRACTAL_DEPTH {
            return vec![];
        }

        let base = self.0;
        let shift = 64 - (depth + 1) * 4;
        
        (0..16) // 2^4 = 16 children per shard
            .map(|i| FractalShardId(base | (i << shift)))
            .collect()
    }

    /// Calculate fractal depth of this shard
    pub fn depth(&self) -> u32 {
        if self.0 == 0 {
            return 0;
        }
        
        let mut depth = 0;
        let mut mask = 0xF000000000000000u64; // Top 4 bits
        
        for i in 0..16 {
            if (self.0 & mask) != 0 {
                depth = i + 1;
            }
            mask >>= 4;
        }
        
        depth
    }

    /// Calculate Hausdorff measure for this shard
    pub fn hausdorff_measure(&self) -> f64 {
        let depth = self.depth() as f64;
        let base_measure = 1.0 / (2.0f64).powi(self.depth() as i32);
        base_measure.powf(FRACTAL_DIMENSION - 1.0)
    }

    /// Check if this shard contains a given complex coordinate
    pub fn contains(&self, c: Complex64) -> bool {
        let cache = SHARD_CACHE.read().unwrap();
        if let Some(metadata) = cache.get(self) {
            let (min, max) = metadata.boundary_box;
            c.re >= min.re && c.re <= max.re && c.im >= min.im && c.im <= max.im
        } else {
            // Calculate boundary box on-demand
            let boundary = self.calculate_boundary();
            c.re >= boundary.0.re && c.re <= boundary.1.re && 
            c.im >= boundary.0.im && c.im <= boundary.1.im
        }
    }

    /// Calculate boundary box for this shard in complex plane
    fn calculate_boundary(&self) -> (Complex64, Complex64) {
        let depth = self.depth();
        let scale = 4.0 / (2.0f64).powi(depth as i32);
        
        let shard_index = (self.0 >> (64 - depth * 4)) & 0xF;
        let x_offset = (shard_index & 0x3) as f64 * scale - 2.0;
        let y_offset = (shard_index >> 2) as f64 * scale - 2.0;
        
        (
            Complex64::new(x_offset, y_offset),
            Complex64::new(x_offset + scale, y_offset + scale)
        )
    }

    /// Get the shard responsible for a specific address
    pub fn shard_for_address(address: &[u8; 20]) -> Self {
        // Hash address to complex coordinate
        let hash = blake3::hash(address.as_ref());
        let bytes = hash.as_bytes();
        
        let re = f64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7]
        ]) % 4.0 - 2.0;
        
        let im = f64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]
        ]) % 4.0 - 2.0;
        
        Self::from_coordinate(Complex64::new(re, im), MAX_FRACTAL_DEPTH)
    }
}

/// Calculate Mandelbrot escape iterations for a complex point
fn mandelbrot_escape_iterations(c: Complex64) -> u32 {
    let mut z = Complex64::new(0.0, 0.0);
    
    for i in 0..MANDELBROT_ITERATIONS {
        if z.norm() > ESCAPE_RADIUS {
            return i;
        }
        z = z * z + c;
    }
    
    MANDELBROT_ITERATIONS
}

impl fmt::Display for FractalShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Shard({:016x})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_shard() {
        let root = FractalShardId::root();
        assert_eq!(root.depth(), 0);
        assert_eq!(root.0, 0);
    }

    #[test]
    fn test_shard_depth() {
        let shard = FractalShardId(0x1000000000000000);
        assert_eq!(shard.depth(), 1);
        
        let shard = FractalShardId(0x1234000000000000);
        assert_eq!(shard.depth(), 4);
    }

    #[test]
    fn test_parent_child_relationship() {
        let parent = FractalShardId(0x1000000000000000);
        let children = parent.children();
        
        assert_eq!(children.len(), 16);
        assert_eq!(children[0].parent(), Some(parent));
        assert_eq!(children[0].depth(), 2);
    }

    #[test]
    fn test_mandelbrot_generation() {
        let c = Complex64::new(-0.5, 0.5);
        let shard = FractalShardId::from_coordinate(c, 4);
        
        assert!(shard.depth() <= 4);
        assert!(shard.contains(c));
    }

    #[test]
    fn test_address_mapping() {
        let address = [0x42u8; 20];
        let shard = FractalShardId::shard_for_address(&address);
        
        assert!(shard.depth() <= MAX_FRACTAL_DEPTH);
        
        // Same address should map to same shard
        let shard2 = FractalShardId::shard_for_address(&address);
        assert_eq!(shard, shard2);
    }
}
