// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Statistics about buffer pool usage for monitoring and optimization.
#[derive(Debug, Clone)]
pub struct PoolStatistics {
    /// Number of successful buffer acquisitions from the pool.
    pub hits: u64,
    /// Number of times a new buffer had to be allocated.
    pub misses: u64,
    /// Current number of buffers stored in the pool.
    pub current_buffers: usize,
    /// Total memory currently used by pooled buffers (in bytes).
    pub total_memory: usize,
    /// Ratio of hits to total requests (hits + misses).
    pub hit_rate: f64,
    /// Average buffer size in the pool.
    pub avg_buffer_size: f64,
    /// Largest buffer size in the pool.
    pub max_buffer_size: usize,
    /// Smallest buffer size in the pool.
    pub min_buffer_size: usize,
}

impl PoolStatistics {
    /// Create new pool statistics.
    pub fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            current_buffers: 0,
            total_memory: 0,
            hit_rate: 0.0,
            avg_buffer_size: 0.0,
            max_buffer_size: 0,
            min_buffer_size: 0,
        }
    }

    /// Update hit rate calculation.
    pub(crate) fn update_hit_rate(&mut self) {
        let total = self.hits + self.misses;
        self.hit_rate = if total > 0 { self.hits as f64 / total as f64 } else { 0.0 };
    }
}

impl Default for PoolStatistics {
    fn default() -> Self {
        Self::new()
    }
}
