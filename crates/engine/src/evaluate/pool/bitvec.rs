// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! BitVec buffer pools for efficient reuse of bit vector allocations.
//!
//! BitVecs are heavily used for null masks and boolean columns, making them
//! prime candidates for pooling to reduce allocation overhead.

use super::{BufferPool, PoolConfig, PoolStats, PooledBuffer};
use reifydb_core::BitVec;
use std::cell::RefCell;
use std::rc::Rc;

/// Specialized pool for BitVec allocations with size-based bucketing.
#[derive(Debug)]
pub struct BitVecPool {
    /// Different size buckets for efficient reuse
    pools: [RefCell<Vec<BitVec>>; 6],
    config: PoolConfig,
    stats: RefCell<PoolStats>,
}

impl BitVecPool {
    /// Size thresholds for different buckets (in number of bits).
    const SIZE_THRESHOLDS: [usize; 6] = [8, 32, 128, 256, 512, 1024];

    /// Create a new BitVec pool with the given configuration.
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
            stats: RefCell::new(PoolStats::new()),
        }
    }

    /// Determine which bucket a given capacity should use.
    fn bucket_for_capacity(&self, capacity: usize) -> usize {
        for (i, &threshold) in Self::SIZE_THRESHOLDS.iter().enumerate() {
            if capacity <= threshold {
                return i;
            }
        }
        Self::SIZE_THRESHOLDS.len() - 1
    }

    /// Return a BitVec to the appropriate size bucket.
    pub fn return_bitvec(&self, bitvec: BitVec) {
        let capacity = bitvec.capacity();
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Create a cleared BitVec for the pool - use capacity bits, all set to false
        let cleared_bitvec = BitVec::new(capacity, false);

        // Return to the appropriate bucket if under limit
        let bucket = &self.pools[bucket_idx];
        let mut bucket_guard = bucket.borrow_mut();
        if bucket_guard.len() < self.config.max_buffers_per_bucket {
            bucket_guard.push(cleared_bitvec);
        }
        // If over limit, just drop the BitVec (let it be deallocated)
    }

    /// Get a BitVec from the appropriate size bucket.
    fn get_from_bucket(&self, capacity: usize) -> Option<BitVec> {
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Try the exact bucket first
        let bucket = &self.pools[bucket_idx];
        // Just take any BitVec from this bucket since they're pre-sized
        if let Some(bitvec) = bucket.borrow_mut().pop() {
            return Some(bitvec);
        }

        // If no suitable BitVec in the exact bucket, try larger buckets
        for larger_bucket_idx in (bucket_idx + 1)..self.pools.len() {
            let bucket = &self.pools[larger_bucket_idx];
            if let Some(bitvec) = bucket.borrow_mut().pop() {
                return Some(bitvec);
            }
        }
        None
    }

    /// Update pool statistics.
    fn update_stats(&self, hit: bool) {
        let mut stats = self.stats.borrow_mut();
        if hit {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
        stats.update_hit_rate();
    }

    /// Calculate current memory usage across all buckets.
    fn calculate_memory_usage(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_borrow() {
                total += bucket_guard
                    .iter()
                    .map(|bv| bv.len() / 8) // Convert bits to bytes (approximate)
                    .sum::<usize>();
            }
        }

        total
    }

    /// Count total BitVecs across all buckets.
    fn count_bitvecs(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            total += bucket.try_borrow().map(|v| v.len()).unwrap_or(0);
        }

        total
    }
}

impl BufferPool<bool> for BitVecPool {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        // Try to get a BitVec from the pool first
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.update_stats(true); // Hit

            // Use the pooled BitVec capacity as hint, but create new Vec<bool>
            let actual_capacity = bitvec.len().max(capacity);
            let buffer = Vec::with_capacity(actual_capacity);
            PooledBuffer::new(buffer, Rc::new(RefCell::new(BitVecPoolWrapper::new(self))))
        } else {
            // Allocate a new buffer
            let buffer = Vec::with_capacity(capacity);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(BitVecPoolWrapper::new(self))))
        }
    }

    fn acquire_exact(&self, size: usize) -> PooledBuffer<bool> {
        // For exact size, we try to reuse but create new if needed
        if let Some(bitvec) = self.get_from_bucket(size) {
            self.update_stats(true); // Hit

            // Create a Vec<bool> with exact size, using pooled capacity as hint
            let actual_capacity = bitvec.len().max(size);
            let mut buffer = Vec::with_capacity(actual_capacity);
            buffer.resize(size, false);
            PooledBuffer::new(buffer, Rc::new(RefCell::new(BitVecPoolWrapper::new(self))))
        } else {
            // Allocate exactly what was requested
            let mut buffer = Vec::with_capacity(size);
            buffer.resize(size, false);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(BitVecPoolWrapper::new(self))))
        }
    }

    fn stats(&self) -> PoolStats {
        let mut stats = self.stats.borrow_mut();
        stats.current_buffers = self.count_bitvecs();
        stats.total_memory = self.calculate_memory_usage();

        // Calculate buffer size statistics
        let mut sizes = Vec::new();

        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_borrow() {
                sizes.extend(bucket_guard.iter().map(|bv| bv.len()));
            }
        }

        if !sizes.is_empty() {
            stats.avg_buffer_size = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
            stats.max_buffer_size = *sizes.iter().max().unwrap_or(&0);
            stats.min_buffer_size = *sizes.iter().min().unwrap_or(&0);
        }

        stats.clone()
    }

    fn clear(&self) {
        for bucket in &self.pools {
            bucket.borrow_mut().clear();
        }

        *self.stats.borrow_mut() = PoolStats::new();
    }

    fn try_return_buffer(&self, buffer: Vec<bool>) {
        // Convert Vec<bool> back to BitVec for returning to pool
        // We'll create a BitVec with the same capacity as the original Vec
        let capacity = buffer.capacity();
        let bitvec = BitVec::new(capacity, false); // Use new() so it has proper length
        self.return_bitvec(bitvec);
    }
}

/// Specialized pool methods for BitVec operations.
impl BitVecPool {
    /// Acquire a BitVec directly (not wrapped in PooledBuffer).
    /// This is more efficient when working directly with BitVec operations.
    pub fn acquire_bitvec(&self, capacity: usize) -> BitVec {
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.update_stats(true);
            if bitvec.len() >= capacity { bitvec } else { BitVec::with_capacity(capacity) }
        } else {
            self.update_stats(false);
            BitVec::with_capacity(capacity)
        }
    }
}

/// Wrapper to allow the pool to be used as a trait object.
struct BitVecPoolWrapper {
    pool_ptr: *const BitVecPool,
}

impl BitVecPoolWrapper {
    fn new(pool: &BitVecPool) -> Self {
        Self { pool_ptr: pool as *const BitVecPool }
    }

    fn pool(&self) -> &BitVecPool {
        unsafe { &*self.pool_ptr }
    }
}

unsafe impl Send for BitVecPoolWrapper {}
unsafe impl Sync for BitVecPoolWrapper {}

impl BufferPool<bool> for BitVecPoolWrapper {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        self.pool().acquire(capacity)
    }

    fn acquire_exact(&self, size: usize) -> PooledBuffer<bool> {
        self.pool().acquire_exact(size)
    }

    fn stats(&self) -> PoolStats {
        self.pool().stats()
    }

    fn clear(&self) {
        self.pool().clear()
    }

    fn try_return_buffer(&self, buffer: Vec<bool>) {
        self.pool().try_return_buffer(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitvec_pool_bucket_selection() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        assert_eq!(pool.bucket_for_capacity(8), 0); // Small bucket
        assert_eq!(pool.bucket_for_capacity(32), 1); // Medium bucket
        assert_eq!(pool.bucket_for_capacity(128), 2); // Large bucket
        assert_eq!(pool.bucket_for_capacity(512), 4); // Very large bucket
        assert_eq!(pool.bucket_for_capacity(1024), 5); // Max bucket
    }

    #[test]
    fn test_bitvec_pool_basic_operations() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        // Acquire a buffer
        let buffer1 = pool.acquire(100);
        assert!(buffer1.capacity() >= 100);

        // Acquire exact size
        let buffer2 = pool.acquire_exact(50);
        assert_eq!(buffer2.len(), 50);

        // Check stats
        let stats = pool.stats();
        assert_eq!(stats.misses, 2); // Both were misses since pool was empty
    }

    #[test]
    fn test_bitvec_pool_direct_bitvec_operations() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        let bitvec1 = pool.acquire_bitvec(100);
        // Return it to pool
        pool.return_bitvec(bitvec1);

        // Acquire another - should reuse
        let bitvec2 = pool.acquire_bitvec(100);
        assert!(!bitvec2.get(50)); // Should be cleared when returned to pool

        let stats = pool.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1); // Should have at least one hit from reuse
    }

    #[test]
    fn test_bitvec_pool_stats() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        // Generate some activity
        for _ in 0..10 {
            let _buffer = pool.acquire(100);
        }

        let stats = pool.stats();
        assert_eq!(stats.hits + stats.misses, 10);
        assert!(stats.hit_rate >= 0.0 && stats.hit_rate <= 1.0);
    }

    #[test]
    fn test_bitvec_pool_clear() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        // Add some buffers to the pool
        {
            let _buffer = pool.acquire(100);
        }

        pool.clear();
        let stats = pool.stats();
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.total_memory, 0);
    }

    #[test]
    fn test_bitvec_pool_size_fallback() {
        let mut pool = BitVecPool::new(PoolConfig::default());

        // Add a large BitVec to a higher bucket
        let large = pool.acquire_bitvec(1024);
        pool.return_bitvec(large);

        // Request a smaller size - should reuse the larger buffer
        let smaller = pool.acquire(128);
        assert!(smaller.capacity() >= 128);

        let stats = pool.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1); // Should reuse the larger buffer
    }
}
