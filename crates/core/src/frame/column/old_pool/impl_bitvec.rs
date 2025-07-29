// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{BufferedPool, PoolConfig, PoolStatistics, PooledBuffer};
use crate::BitVec;
use crate::frame::POOL_SIZE_THRESHOLDS;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct BitVecPool {
    pools: [RefCell<Vec<BitVec>>; 6],
    config: PoolConfig,
    stats: RefCell<PoolStatistics>,
}

impl BitVecPool {
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

    fn bucket_for_capacity(&self, capacity: usize) -> usize {
        for (i, &threshold) in POOL_SIZE_THRESHOLDS.iter().enumerate() {
            if capacity <= threshold {
                return i;
            }
        }
        POOL_SIZE_THRESHOLDS.len() - 1
    }

    fn update_stats(&self, hit: bool) {
        let mut stats = self.stats.borrow_mut();
        if hit {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
        stats.update_hit_rate();
    }

    fn count(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            total += bucket.try_borrow().map(|v| v.len()).unwrap_or(0);
        }

        total
    }

    pub fn clear(&self) {
        for bucket in &self.pools {
            bucket.borrow_mut().clear();
        }
        *self.stats.borrow_mut() = PoolStatistics::new();
    }

    fn return_to_bucket(&self, item: BitVec, capacity: usize) {
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Return to pool if under limit
        let bucket = &self.pools[bucket_idx];
        let mut bucket_guard = bucket.borrow_mut();
        if bucket_guard.len() < self.config.max_buffers_per_bucket {
            bucket_guard.push(item);
        }
        // If over limit, just drop the item (let it be deallocated)
    }

    pub fn return_bitvec(&self, bitvec: BitVec) {
        let capacity = bitvec.capacity();

        // Create a cleared BitVec for the pool - use capacity bits, all set to false
        let cleared_bitvec = BitVec::repeat(capacity, false);
        self.return_to_bucket(cleared_bitvec, capacity);
    }

    fn get_from_bucket(&self, capacity: usize) -> Option<BitVec> {
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Try the exact bucket first
        let bucket = &self.pools[bucket_idx];
        // Respect max_buffers_per_bucket limit when acquiring
        let mut bucket_guard = bucket.borrow_mut();
        let available_count = bucket_guard.len().min(self.config.max_buffers_per_bucket);
        if available_count > 0 {
            return bucket_guard.pop();
        }
        drop(bucket_guard);

        // If no suitable BitVec in the exact bucket, try larger buckets
        for larger_bucket_idx in (bucket_idx + 1)..self.pools.len() {
            let bucket = &self.pools[larger_bucket_idx];
            let mut bucket_guard = bucket.borrow_mut();
            let available_count = bucket_guard.len().min(self.config.max_buffers_per_bucket);
            if available_count > 0 {
                return bucket_guard.pop();
            }
        }
        None
    }

    fn calculate_memory_usage(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_borrow() {
                total += bucket_guard.iter().map(|bv| bv.len() / 8).sum::<usize>();
            }
        }

        total
    }
}

impl BufferedPool<bool> for BitVecPool {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        // Try to get a BitVec from the pool first
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.update_stats(true); // Hit

            // Use the pooled BitVec capacity as hint, but create new Vec<bool>
            let actual_capacity = bitvec.len().max(capacity);
            let buffer = Vec::with_capacity(actual_capacity);
            PooledBuffer::new(buffer, Rc::new(RefCell::new(Wrapper::new(self))))
        } else {
            // Allocate a new buffer
            let buffer = Vec::with_capacity(capacity);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(Wrapper::new(self))))
        }
    }

    fn stats(&self) -> PoolStatistics {
        let mut stats = self.stats.borrow_mut();
        stats.current_buffers = self.count();
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
        *self.stats.borrow_mut() = PoolStatistics::new();
    }

    fn release(&self, buffer: Vec<bool>) {
        // Convert Vec<bool> back to BitVec for returning to pool
        // We'll create a BitVec with the same capacity as the original Vec
        let capacity = buffer.capacity();
        let bitvec = BitVec::repeat(capacity, false); // Use new() so it has proper length
        self.return_bitvec(bitvec);
    }
}

impl BitVecPool {
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

impl BufferedPool<bool> for Wrapper {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        self.pool().acquire(capacity)
    }

    fn stats(&self) -> PoolStatistics {
        self.pool().stats()
    }

    fn clear(&self) {
        self.pool().clear()
    }

    fn release(&self, buffer: Vec<bool>) {
        self.pool().release(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitvec_pool_bucket_selection() {
        let pool = BitVecPool::new(PoolConfig::default());

        assert_eq!(pool.bucket_for_capacity(8), 0); // Small bucket
        assert_eq!(pool.bucket_for_capacity(32), 1); // Medium bucket
        assert_eq!(pool.bucket_for_capacity(128), 2); // Large bucket
        assert_eq!(pool.bucket_for_capacity(512), 4); // Very large bucket
        assert_eq!(pool.bucket_for_capacity(1024), 5); // Max bucket
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
