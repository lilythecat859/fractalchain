// fractalchain/state/src/state_expiry.rs
//! State expiry mechanism with fractal garbage collection
//! Implements 1-year state expiry with archive support

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fractalchain_types::{ShardId, FractalError};

/// State expiry time: 1 year
pub const STATE_EXPIRY_TIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// State warning time: 11 months
pub const STATE_WARNING_TIME: Duration = Duration::from_secs(335 * 24 * 60 * 60);
/// Archive check interval: 1 week
pub const ARCHIVE_CHECK_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Maximum archive size: 1GB
pub const MAX_ARCHIVE_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateExpiryManager {
    /// Active state tracking
    pub active_state: HashMap<[u8; 32], StateMetadata>,
    /// Expiring state queue
    pub expiring_queue: VecDeque<ExpiryEntry>,
    /// Archive state references
    pub archive_state: ArchiveState,
    /// Fractal expiry tracking
    pub fractal_expiry: FractalExpiryTracker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMetadata {
    /// Last access timestamp
    pub last_access: u64,
    /// Creation timestamp
    pub created: u64,
    /// Access frequency (for LRU)
    pub access_count: u64,
    /// Shard assignment
    pub shard_id: ShardId,
    /// State size in bytes
    pub size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryEntry {
    /// State key
    pub state_key: [u8; 32],
    /// Expiry timestamp
    pub expiry_time: u64,
    /// Shard ID
    pub shard_id: ShardId,
    /// Archive reference
    pub archive_ref: Option<ArchiveReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveState {
    /// Archived state references
    pub archived: HashMap<[u8; 32], ArchiveReference>,
    /// Archive size tracking
    pub total_size: usize,
    /// Last archive timestamp
    pub last_archive: u64,
    /// Archive fragmentation score
    pub fragmentation_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveReference {
    /// Archive storage location (IPFS hash, file path, etc.)
    pub location: [u8; 32],
    /// Archive format version
    pub format_version: u32,
    /// Compression type
    pub compression: CompressionType,
    /// Original size
    pub original_size: usize,
    /// Archived size
    pub archived_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Brotli,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalExpiryTracker {
    /// Shard-based expiry scheduling
    pub shard_expiry: HashMap<ShardId, ShardExpiryInfo>,
    /// Fractal garbage collection schedule
    pub gc_schedule: GcSchedule,
    /// Cross-shard expiry coordination
    pub cross_shard_coordination: CrossShardExpiry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardExpiryInfo {
    /// State count in shard
    pub state_count: usize,
    /// Total size
    pub total_size: usize,
    /// Expiry rate
    pub expiry_rate: f64,
    /// Last GC timestamp
    pub last_gc: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcSchedule {
    /// Next GC timestamp
    pub next_gc: u64,
    /// GC interval
    pub interval: u64,
    /// Fractal GC rounds
    pub fractal_rounds: Vec<FractalGcRound>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FractalGcRound {
    /// Round number
    pub round: u32,
    /// Shards involved
    pub shards: Vec<ShardId>,
    /// GC type
    pub gc_type: GcType,
    /// Estimated duration
    pub estimated_duration: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GcType {
    Light,  // Remove only expired state
    Medium, // Remove expired and warning state
    Deep,   // Full garbage collection with defragmentation
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardExpiry {
    /// Cross-shard expiry coordination messages
    pub coordination_queue: Vec<CoordinationMessage>,
    /// Pending cross-shard operations
    pub pending_operations: HashMap<[u8; 32], PendingExpiryOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationMessage {
    /// Message type
    pub msg_type: CoordinationType,
    /// Source shard
    pub source_shard: ShardId,
    /// Target shards
    pub target_shards: Vec<ShardId>,
    /// State keys involved
    pub state_keys: Vec<[u8; 32]>,
    /// Timestamp
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinationType {
    ExpiryWarning,
    ArchiveRequest,
    ArchiveResponse,
    CrossShardExpiry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingExpiryOp {
    /// Operation type
    pub op_type: PendingOpType,
    /// State keys
    pub state_keys: Vec<[u8; 32]>,
    /// Timeout
    pub timeout: u64,
    /// Retry count
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PendingOpType {
    Archive,
    Expire,
    CrossShardCoordination,
}

impl StateExpiryManager {
    /// Create new state expiry manager
    pub fn new() -> Self {
        StateExpiryManager {
            active_state: HashMap::new(),
            expiring_queue: VecDeque::new(),
            archive_state: ArchiveState {
                archived: HashMap::new(),
                total_size: 0,
                last_archive: current_timestamp(),
                fragmentation_score: 0.0,
            },
            fractal_expiry: FractalExpiryTracker {
                shard_expiry: HashMap::new(),
                gc_schedule: GcSchedule {
                    next_gc: current_timestamp() + ARCHIVE_CHECK_INTERVAL.as_secs(),
                    interval: ARCHIVE_CHECK_INTERVAL.as_secs(),
                    fractal_rounds: Vec::new(),
                },
                cross_shard_coordination: CrossShardExpiry {
                    coordination_queue: Vec::new(),
                    pending_operations: HashMap::new(),
                },
            },
        }
    }

    /// Track state access for LRU management
    pub fn track_access(&mut self, state_key: [u8; 32], shard_id: ShardId) {
        if let Some(metadata) = self.active_state.get_mut(&state_key) {
            metadata.last_access = current_timestamp();
            metadata.access_count += 1;
        } else {
            // New state tracking
            let metadata = StateMetadata {
                last_access: current_timestamp(),
                created: current_timestamp(),
                access_count: 1,
                shard_id,
                size: 32, // Default size
            };
            
            self.active_state.insert(state_key, metadata);
            
            // Check if state is approaching expiry
            if self.is_approaching_expiry(&metadata) {
                let expiry_entry = ExpiryEntry {
                    state_key,
                    expiry_time: metadata.created + STATE_EXPIRY_TIME.as_secs(),
                    shard_id,
                    archive_ref: None,
                };
                
                self.expiring_queue.push_back(expiry_entry);
            }
        }
        
        // Update shard expiry info
        self.update_shard_expiry_info(shard_id);
    }

    /// Check if state is approaching expiry
    fn is_approaching_expiry(&self, metadata: &StateMetadata) -> bool {
        let current_time = current_timestamp();
        let age = current_time - metadata.created;
        
        age > STATE_WARNING_TIME.as_secs()
    }

    /// Update shard expiry information
    fn update_shard_expiry_info(&mut self, shard_id: ShardId) {
        let info = self.fractal_expiry.shard_expiry.entry(shard_id)
            .or_insert_with(|| ShardExpiryInfo {
                state_count: 0,
                total_size: 0,
                expiry_rate: 0.0,
                last_gc: 0,
            });
        
        info.state_count += 1;
        info.total_size += 32; // Default size
    }

    /// Perform garbage collection with fractal coordination
    pub async fn perform_garbage_collection(&mut self) -> Result<Vec<[u8; 32]>, FractalError> {
        let current_time = current_timestamp();
        
        // Check if GC is scheduled
        if current_time < self.fractal_expiry.gc_schedule.next_gc {
            return Ok(Vec::new());
        }
        
        // Determine GC type based on fragmentation and expiry rates
        let gc_type = self.determine_gc_type();
        
        // Perform fractal GC rounds
        let expired_keys = self.perform_fractal_gc_rounds(gc_type).await?;
        
        // Update next GC schedule
        self.fractal_expiry.gc_schedule.next_gc = current_time + self.fractal_expiry.gc_schedule.interval;
        
        Ok(expired_keys)
    }

    /// Determine garbage collection type
    fn determine_gc_type(&self) -> GcType {
        let fragmentation = self.archive_state.fragmentation_score;
        let expiry_rate = self.calculate_average_expiry_rate();
        
        if fragmentation > 0.8 || expiry_rate > 0.7 {
            GcType::Deep
        } else if fragmentation > 0.5 || expiry_rate > 0.4 {
            GcType::Medium
        } else {
            GcType::Light
        }
    }

    /// Calculate average expiry rate across all shards
    fn calculate_average_expiry_rate(&self) -> f64 {
        let total_shards = self.fractal_expiry.shard_expiry.len();
        if total_shards == 0 {
            return 0.0;
        }
        
        let total_rate: f64 = self.fractal_expiry.shard_expiry.values()
            .map(|info| info.expiry_rate)
            .sum();
        
        total_rate / total_shards as f64
    }

    /// Perform fractal garbage collection rounds
    async fn perform_fractal_gc_rounds(&mut self, gc_type: GcType) -> Result<Vec<[u8; 32]>, FractalError> {
        let mut expired_keys = Vec::new();
        let mut rounds = self.generate_fractal_gc_schedule(gc_type);
        
        for round in &mut rounds {
            // Perform GC for specified shards
            for shard_id in &round.shards {
                let shard_expired = self.perform_shard_gc(*shard_id, &gc_type).await?;
                expired_keys.extend(shard_expired);
            }
            
            // Coordinate cross-shard GC if needed
            if expired_keys.len() > 100 { // Threshold for coordination
                self.coordinate_cross_shard_gc(&expired_keys, *shard_id).await?;
            }
        }
        
        Ok(expired_keys)
    }

    /// Generate fractal GC schedule
    fn generate_fractal_gc_schedule(&self, gc_type: GcType) -> Vec<FractalGcRound> {
        let mut rounds = Vec::new();
        let shards: Vec<ShardId> = self.fractal_expiry.shard_expiry.keys().copied().collect();
        
        // Group shards by fractal depth for efficient GC
        let mut shards_by_depth: HashMap<u8, Vec<ShardId>> = HashMap::new();
        for shard in shards {
            let depth = shard.depth();
            shards_by_depth.entry(depth).or_insert_with(Vec::new).push(shard);
        }
        
        // Create GC rounds
        for (depth, depth_shards) in shards_by_depth {
            let round = FractalGcRound {
                round: depth as u32,
                shards: depth_shards,
                gc_type: gc_type.clone(),
                estimated_duration: self.estimate_gc_duration(&gc_type),
            };
            
            rounds.push(round);
        }
        
        rounds
    }

    /// Estimate GC duration based on type and shard count
    fn estimate_gc_duration(&self, gc_type: &GcType) -> u64 {
        match gc_type {
            GcType::Light => 60,      // 1 minute
            GcType::Medium => 300,    // 5 minutes
            GcType::Deep => 1800,     // 30 minutes
        }
    }

    /// Perform garbage collection for specific shard
    async fn perform_shard_gc(
        &mut self,
        shard_id: ShardId,
        gc_type: &GcType,
    ) -> Result<Vec<[u8; 32]>, FractalError> {
        let mut expired_keys = Vec::new();
        let current_time = current_timestamp();
        
        // Process expiring queue for this shard
        let mut i = 0;
        while i < self.expiring_queue.len() {
            if let Some(entry) = self.expiring_queue.get(i) {
                if entry.shard_id == shard_id && entry.expiry_time <= current_time {
                    let entry = self.expiring_queue.remove(i).unwrap();
                    
                    // Archive state before expiry
                    self.archive_state(entry.state_key, entry.shard_id).await?;
                    
                    // Remove from active state
                    self.active_state.remove(&entry.state_key);
                    expired_keys.push(entry.state_key);
                    
                    continue;
                }
            }
            i += 1;
        }
        
        // Update shard expiry info
        if let Some(info) = self.fractal_expiry.shard_expiry.get_mut(&shard_id) {
            info.last_gc = current_time;
            info.expiry_rate = expired_keys.len() as f64 / info.state_count.max(1) as f64;
        }
        
        Ok(expired_keys)
    }

    /// Archive state before expiry
    async fn archive_state(
        &mut self,
        state_key: [u8; 32],
        shard_id: ShardId,
    ) -> Result<(), FractalError> {
        // Get state metadata
        let metadata = match self.active_state.get(&state_key) {
            Some(meta) => meta.clone(),
            None => return Ok(()), // State already removed
        };
        
        // Create archive reference
        let archive_ref = ArchiveReference {
            location: self.generate_archive_location(&state_key, shard_id),
            format_version: 1,
            compression: CompressionType::Zstd,
            original_size: metadata.size,
            archived_size: metadata.size / 2, // Assume 50% compression
        };
        
        // Add to archive state
        self.archive_state.archived.insert(state_key, archive_ref.clone());
        self.archive_state.total_size += archive_ref.archived_size;
        self.archive_state.last_archive = current_timestamp();
        
        // Check archive size limits
        if self.archive_state.total_size > MAX_ARCHIVE_SIZE {
            self.perform_archive_cleanup().await?;
        }
        
        Ok(())
    }

    /// Generate archive location (IPFS hash simulation)
    fn generate_archive_location(&self, state_key: &[u8; 32], shard_id: ShardId) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        
        let mut hasher = Sha256::new();
        hasher.update(state_key);
        hasher.update(&shard_id.as_u64().to_le_bytes());
        hasher.update(&current_timestamp().to_le_bytes());
        
        hasher.finalize().into()
    }

    /// Perform archive cleanup when size limit exceeded
    async fn perform_archive_cleanup(&mut self) -> Result<(), FractalError> {
        // Remove oldest archived state
        let mut oldest_key = None;
        let mut oldest_time = u64::MAX;
        
        for (key, archive_ref) in &self.archive_state.archived {
            // Simulate archive timestamp from location hash
            let archive_time = u64::from_le_bytes([
                archive_ref.location[0], archive_ref.location[1], 
                archive_ref.location[2], archive_ref.location[3],
                archive_ref.location[4], archive_ref.location[5], 
                archive_ref.location[6], archive_ref.location[7],
            ]);
            
            if archive_time < oldest_time {
                oldest_time = archive_time;
                oldest_key = Some(*key);
            }
        }
        
        if let Some(key) = oldest_key {
            if let Some(archive_ref) = self.archive_state.archived.remove(&key) {
                self.archive_state.total_size -= archive_ref.archived_size;
            }
        }
        
        // Update fragmentation score
        self.archive_state.fragmentation_score = 
            1.0 - (self.archive_state.archived.len() as f64 / 
                   (self.archive_state.archived.len() + 100) as f64);
        
        Ok(())
    }

    /// Coordinate cross-shard garbage collection
    async fn coordinate_cross_shard_gc(
        &mut self,
        expired_keys: &[u8; 32],
        source_shard: ShardId,
    ) -> Result<(), FractalError> {
        // Create coordination message
        let coord_msg = CoordinationMessage {
            msg_type: CoordinationType::CrossShardExpiry,
            source_shard,
            target_shards: vec![], // Would be determined by expired keys
            state_keys: expired_keys.to_vec(),
            timestamp: current_timestamp(),
        };
        
        // Add to coordination queue
        self.fractal_expiry.cross_shard_coordination.coordination_queue.push(coord_msg);
        
        Ok(())
    }

    /// Restore archived state
    pub async fn restore_archived_state(
        &mut self,
        state_key: [u8; 32],
    ) -> Result<Option<Vec<u8>>, FractalError> {
        if let Some(archive_ref) = self.archive_state.archived.get(&state_key) {
            // Simulate state restoration from archive
            // In real implementation, this would fetch from IPFS/file system
            
            let restored_data = vec![0u8; archive_ref.original_size]; // Placeholder
            
            // Remove from archive
            self.archive_state.archived.remove(&state_key);
            self.archive_state.total_size -= archive_ref.archived_size;
            
            Ok(Some(restored_data))
        } else {
            Ok(None)
        }
    }

    /// Get expiry statistics
    pub fn get_expiry_stats(&self) -> ExpiryStats {
        let total_state = self.active_state.len();
        let archived_state = self.archive_state.archived.len();
        let expiring_soon = self.expiring_queue.len();
        
        let total_size = self.active_state.values()
            .map(|meta| meta.size)
            .sum::<usize>() + self.archive_state.total_size;
        
        ExpiryStats {
            total_state,
            archived_state,
            expiring_soon,
            total_size,
            average_expiry_rate: self.calculate_average_expiry_rate(),
            fragmentation_score: self.archive_state.fragmentation_score,
        }
    }
}

/// Expiry statistics
#[derive(Debug, Clone)]
pub struct ExpiryStats {
    pub total_state: usize,
    pub archived_state: usize,
    pub expiring_soon: usize,
    pub total_size: usize,
    pub average_expiry_rate: f64,
    pub fragmentation_score: f64,
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_expiry_manager_creation() {
        let manager = StateExpiryManager::new();
        assert_eq!(manager.active_state.len(), 0);
        assert_eq!(manager.expiring_queue.len(), 0);
    }

    #[test]
    fn test_state_tracking() {
        let mut manager = StateExpiryManager::new();
        let state_key = [0xABu8; 32];
        let shard_id = ShardId(1);
        
        manager.track_access(state_key, shard_id);
        
        assert!(manager.active_state.contains_key(&state_key));
        assert_eq!(manager.fractal_expiry.shard_expiry.get(&shard_id).unwrap().state_count, 1);
    }

    #[tokio::test]
    async fn test_garbage_collection() {
        let mut manager = StateExpiryManager::new();
        
        // Add old state
        let old_key = [0xCDu8; 32];
        manager.active_state.insert(old_key, StateMetadata {
            last_access: current_timestamp() - STATE_EXPIRY_TIME.as_secs() - 1000,
            created: current_timestamp() - STATE_EXPIRY_TIME.as_secs() - 2000,
            access_count: 1,
            shard_id: ShardId(1),
            size: 32,
        });
        
        manager.expiring_queue.push_back(ExpiryEntry {
            state_key: old_key,
            expiry_time: current_timestamp() - 100,
            shard_id: ShardId(1),
            archive_ref: None,
        });
        
        let expired = manager.perform_garbage_collection().await.unwrap();
        assert!(expired.contains(&old_key));
    }

    #[test]
    fn test_expiry_stats() {
        let manager = StateExpiryManager::new();
        let stats = manager.get_expiry_stats();
        
        assert_eq!(stats.total_state, 0);
        assert_eq!(stats.archived_state, 0);
        assert_eq!(stats.expiring_soon, 0);
    }
}