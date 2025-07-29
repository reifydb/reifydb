// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! BitVec buffer pools for efficient reuse of bit vector allocations.
use super::{BufferPool,  PoolConfig, PoolStatistics, PooledBuffer};
use crate::BitVec;
use std::cell::RefCell;
use std::rc::Rc;
use crate::frame::common::PoolBase;

/// Specialized pool for BitVec allocations with size-based bucketing.
#[derive(Debug)]
pub struct BitVecPool {
    base: PoolBase<BitVec>,
}

impl BitVecPool {
    /// Create a new BitVec pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            base: PoolBase::new(config),
        }
    }

    /// Return a BitVec to the appropriate size bucket.
    pub fn return_bitvec(&self, bitvec: BitVec) {
        let capacity = bitvec.capacity();

        // Create a cleared BitVec for the pool - use capacity bits, all set to false
        let cleared_bitvec = BitVec::repeat(capacity, false);
        self.base.return_to_bucket(cleared_bitvec, capacity);
    }

    /// Get a BitVec from the appropriate size bucket.
    fn get_from_bucket(&self, capacity: usize) -> Option<BitVec> {
        let bucket_idx = self.base.bucket_for_capacity(capacity);

        // Try the exact bucket first
        let bucket = &self.base.pools[bucket_idx];
        // Respect max_buffers_per_bucket limit when acquiring
        let mut bucket_guard = bucket.borrow_mut();
        let available_count = bucket_guard.len().min(self.base.config.max_buffers_per_bucket);
        if available_count > 0 {
            return bucket_guard.pop();
        }
        drop(bucket_guard);

        // If no suitable BitVec in the exact bucket, try larger buckets
        for larger_bucket_idx in (bucket_idx + 1)..self.base.pools.len() {
            let bucket = &self.base.pools[larger_bucket_idx];
            let mut bucket_guard = bucket.borrow_mut();
            let available_count = bucket_guard.len().min(self.base.config.max_buffers_per_bucket);
            if available_count > 0 {
                return bucket_guard.pop();
            }
        }
        None
    }

    /// Calculate current memory usage across all buckets.
    fn calculate_memory_usage(&self) -> usize {
        let mut total = 0;

        for bucket in &self.base.pools {
            if let Ok(bucket_guard) = bucket.try_borrow() {
                total += bucket_guard.iter().map(|bv| bv.len() / 8).sum::<usize>();
            }
        }

        total
    }
}

impl BufferPool<bool> for BitVecPool {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        // Try to get a BitVec from the pool first
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.base.update_stats(true); // Hit

            // Use the pooled BitVec capacity as hint, but create new Vec<bool>
            let actual_capacity = bitvec.len().max(capacity);
            let buffer = Vec::with_capacity(actual_capacity);
            PooledBuffer::new(buffer, Rc::new(RefCell::new(Wrapper::new(self))))
        } else {
            // Allocate a new buffer
            let buffer = Vec::with_capacity(capacity);
            self.base.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(Wrapper::new(self))))
        }
    }

    fn stats(&self) -> PoolStatistics {
        let mut stats = self.base.stats.borrow_mut();
        stats.current_buffers = self.base.count();
        stats.total_memory = self.calculate_memory_usage();

        // Calculate buffer size statistics
        let mut sizes = Vec::new();

        for bucket in &self.base.pools {
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
        self.base.clear();
    }

    fn return_buffer(&self, buffer: Vec<bool>) {
        // Convert Vec<bool> back to BitVec for returning to pool
        // We'll create a BitVec with the same capacity as the original Vec
        let capacity = buffer.capacity();
        let bitvec = BitVec::repeat(capacity, false); // Use new() so it has proper length
        self.return_bitvec(bitvec);
    }
}

/// Specialized pool methods for BitVec operations.
impl BitVecPool {
    /// Acquire a BitVec directly (not wrapped in PooledBuffer).
    /// This is more efficient when working directly with BitVec operations.
    pub fn acquire_bitvec(&self, capacity: usize) -> BitVec {
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.base.update_stats(true);
            if bitvec.len() >= capacity { bitvec } else { BitVec::with_capacity(capacity) }
        } else {
            self.base.update_stats(false);
            BitVec::with_capacity(capacity)
        }
    }
}

/// Wrapper to allow the pool to be used as a trait object.
struct Wrapper {
    pool_ptr: *const BitVecPool,
}

impl Wrapper {
    fn new(pool: &BitVecPool) -> Self {
        Self { pool_ptr: pool as *const BitVecPool }
    }

    fn pool(&self) -> &BitVecPool {
        unsafe { &*self.pool_ptr }
    }
}

impl BufferPool<bool> for Wrapper {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        self.pool().acquire(capacity)
    }

    fn stats(&self) -> PoolStatistics {
        self.pool().stats()
    }

    fn clear(&self) {
        self.pool().clear()
    }

    fn return_buffer(&self, buffer: Vec<bool>) {
        self.pool().return_buffer(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitvec_pool_bucket_selection() {
        let pool = BitVecPool::new(PoolConfig::default());

        assert_eq!(pool.base.bucket_for_capacity(8), 0); // Small bucket
        assert_eq!(pool.base.bucket_for_capacity(32), 1); // Medium bucket
        assert_eq!(pool.base.bucket_for_capacity(128), 2); // Large bucket
        assert_eq!(pool.base.bucket_for_capacity(512), 4); // Very large bucket
        assert_eq!(pool.base.bucket_for_capacity(1024), 5); // Max bucket
    }

    #[test]
    fn test_bitvec_pool_basic_operations() {
        let pool = BitVecPool::new(PoolConfig::default());

        let buffer1 = pool.acquire(100);
        assert!(buffer1.capacity() >= 100);
        assert_eq!(buffer1.len(), 0);

        let buffer2 = pool.acquire_bitvec(50);
        assert!(buffer2.capacity() >= 50);
        assert_eq!(buffer2.len(), 0);

        let stats = pool.stats();
        assert_eq!(stats.misses, 2);
    }

    #[test]
    fn test_bitvec_pool_direct_bitvec_operations() {
        let pool = BitVecPool::new(PoolConfig::default());

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
        let pool = BitVecPool::new(PoolConfig::default());

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
        let pool = BitVecPool::new(PoolConfig::default());

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
        let pool = BitVecPool::new(PoolConfig::default());

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