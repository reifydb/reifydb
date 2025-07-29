// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{BufferedPool, PoolConfig, PoolStatistics, PooledBuffer};
use crate::frame::POOL_SIZE_THRESHOLDS;
use crate::value::IsNumber;
use std::cell::RefCell;
use std::mem::size_of;
use std::rc::Rc;

#[derive(Debug)]
pub struct NumericPool<T>
where
    T: IsNumber + 'static,
{
    pools: [RefCell<Vec<Vec<T>>>; 6],
    config: PoolConfig,
    stats: RefCell<PoolStatistics>,
}

impl<T> NumericPool<T>
where
    T: IsNumber + 'static,
{
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

    pub fn release(&self, mut buffer: Vec<T>) {
        buffer.clear(); // Clear data but keep allocation
        let capacity = buffer.capacity();
        let bucket_idx = self.bucket_for_capacity(capacity);

        // Return to pool if under limit
        let bucket = &self.pools[bucket_idx];
        let mut bucket_guard = bucket.borrow_mut();
        if bucket_guard.len() < self.config.max_buffers_per_bucket {
            bucket_guard.push(buffer);
        }
        // If over limit, just drop the item (let it be deallocated)
    }

    fn get_from_bucket(&self, capacity: usize) -> Option<Vec<T>> {
        let bucket_idx = self.bucket_for_capacity(capacity);
        let bucket = &self.pools[bucket_idx];

        // Find a buffer with sufficient capacity
        let mut bucket_guard = bucket.borrow_mut();
        let available_count = bucket_guard.len().min(self.config.max_buffers_per_bucket);

        for i in (0..available_count).rev() {
            if bucket_guard[i].capacity() >= capacity {
                return Some(bucket_guard.swap_remove(i));
            }
        }
        None
    }

    fn calculate_memory_usage(&self) -> usize {
        let mut total = 0;

        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_borrow() {
                total += bucket_guard.iter().map(|v| v.capacity() * size_of::<T>()).sum::<usize>();
            }
        }

        total
    }
}

impl<T> BufferedPool<T> for NumericPool<T>
where
    T: IsNumber + 'static,
{
    fn acquire(&self, capacity: usize) -> PooledBuffer<T> {
        // Try to get a buffer from the pool first
        if let Some(mut buffer) = self.get_from_bucket(capacity) {
            // Ensure the buffer has the requested capacity
            if buffer.capacity() < capacity {
                buffer.reserve(capacity - buffer.capacity());
            }
            self.update_stats(true); // Hit
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
                sizes.extend(bucket_guard.iter().map(|v| v.capacity()));
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

    fn release(&self, _buffer: Vec<T>) {
    }
}

struct Wrapper<T>
where
    T: IsNumber + 'static,
{
    pool_ptr: *const NumericPool<T>,
}

impl<T> Wrapper<T>
where
    T: IsNumber + 'static,
{
    fn new(pool: &NumericPool<T>) -> Self {
        Self { pool_ptr: pool as *const NumericPool<T> }
    }

    fn pool(&self) -> &NumericPool<T> {
        unsafe { &*self.pool_ptr }
    }
}

impl<T> BufferedPool<T> for Wrapper<T>
where
    T: IsNumber + 'static,
{
    fn acquire(&self, capacity: usize) -> PooledBuffer<T> {
        self.pool().acquire(capacity)
    }

    fn stats(&self) -> PoolStatistics {
        self.pool().stats()
    }

    fn clear(&self) {
        self.pool().clear()
    }

    fn release(&self, buffer: Vec<T>) {
        self.pool().release(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_pool_basic_operations() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

        let buffer1 = pool.acquire(100);
        assert!(buffer1.capacity() >= 100);
        assert_eq!(buffer1.len(), 0);

        let buffer2 = pool.acquire(50);
        assert!(buffer2.capacity() >= 50);
        assert_eq!(buffer2.len(), 0);

        let stats = pool.stats();
        assert_eq!(stats.misses, 2);
    }

    #[test]
    fn test_numeric_pool_reuse() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

        // Acquire and drop a buffer
        {
            let mut buffer = pool.acquire(100);
            buffer.push(42);
            // Buffer will be returned to pool when dropped
        }

        // Acquire another buffer - should reuse the previous allocation
        let buffer = pool.acquire(100);
        assert!(buffer.is_empty()); // Should be cleared when returned to pool

        let stats = pool.stats();
        assert!(stats.hits >= 1); // Should have at least one hit from reuse
    }

    #[test]
    fn test_numeric_pool_size_buckets() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

        // Test different size buckets
        let small = pool.acquire(500); // Should go to small bucket
        let medium = pool.acquire(5000); // Should go to medium bucket
        let large = pool.acquire(50000); // Should go to large bucket

        assert!(small.capacity() >= 500);
        assert!(medium.capacity() >= 5000);
        assert!(large.capacity() >= 50000);
    }

    #[test]
    fn test_numeric_pool_stats() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

        // Generate some activity
        for _ in 0..10 {
            let _buffer = pool.acquire(100);
        }

        let stats = pool.stats();
        assert_eq!(stats.hits + stats.misses, 10);
        assert!(stats.hit_rate >= 0.0 && stats.hit_rate <= 1.0);
    }

    #[test]
    fn test_numeric_pool_clear() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

        // Add some buffers to the pool
        {
            let _buffer = pool.acquire(100);
        }

        pool.clear();
        let stats = pool.stats();
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.total_memory, 0);
    }
}
