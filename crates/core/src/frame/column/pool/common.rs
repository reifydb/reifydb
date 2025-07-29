// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Common pool functionality shared between BitVecPool and NumericPool.

use super::{PoolConfig, PoolStatistics};
use crate::frame::POOL_SIZE_THRESHOLDS;
use std::cell::RefCell;

/// Common pool structure and operations.
#[derive(Debug)]
pub struct PoolBase<T> {
    pub pools: [RefCell<Vec<T>>; 6],
    pub config: PoolConfig,
    pub stats: RefCell<PoolStatistics>,
}

impl<T> PoolBase<T> {
    /// Create a new pool base with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pools: [
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
            ],
            config,
            stats: RefCell::new(PoolStatistics::new()),
        }
    }

    /// Determine which bucket index a given capacity should use.
    pub fn bucket_for_capacity(&self, capacity: usize) -> usize {
        for (i, &threshold) in POOL_SIZE_THRESHOLDS.iter().enumerate() {
            if capacity <= threshold {
                return i;
            }
        }
        POOL_SIZE_THRESHOLDS.len() - 1
    }

    /// Update pool statistics.
    pub fn update_stats(&self, hit: bool) {
        let mut stats = self.stats.borrow_mut();
        if hit {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
        stats.update_hit_rate();
    }

    /// Count total items across all buckets.
    pub fn count(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            total += bucket.try_borrow().map(|v| v.len()).unwrap_or(0);
        }

        total
    }

    /// Clear all buckets and reset statistics.
    pub fn clear(&self) {
        for bucket in &self.pools {
            bucket.borrow_mut().clear();
        }
        *self.stats.borrow_mut() = PoolStatistics::new();
    }

    /// Return an item to the appropriate size bucket if under limit.
    pub fn return_to_bucket(&self, item: T, capacity: usize) {
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Return to pool if under limit
        let bucket = &self.pools[bucket_idx];
        let mut bucket_guard = bucket.borrow_mut();
        if bucket_guard.len() < self.config.max_buffers_per_bucket {
            bucket_guard.push(item);
        }
        // If over limit, just drop the item (let it be deallocated)
    }
}