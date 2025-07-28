// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Numeric buffer pools for efficient reuse of Vec<T> allocations.
//!
//! These pools are size-bucketed to maximize reuse while minimizing memory waste.

use super::{BufferPool, PoolConfig, PoolStats, PooledBuffer};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

/// Size-bucketed pool for numeric types (i8, i16, i32, i64, i128, f32, f64, etc.)
#[derive(Debug)]
pub struct NumericPool<T> {
    small: RefCell<Vec<Vec<T>>>,
    medium: RefCell<Vec<Vec<T>>>,
    large: RefCell<Vec<Vec<T>>>,

    config: PoolConfig,
    stats: RefCell<PoolStats>,
    _phantom: PhantomData<T>,
}

impl<T> NumericPool<T>
where
    T: Default + Clone + Send + Sync + 'static,
{
    /// Create a new numeric pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            small: RefCell::new(Vec::new()),
            medium: RefCell::new(Vec::new()),
            large: RefCell::new(Vec::new()),
            config,
            stats: RefCell::new(PoolStats::new()),
            _phantom: PhantomData,
        }
    }

    /// Return a buffer to the appropriate size bucket.
    pub fn return_buffer(&self, mut buffer: Vec<T>) {
        buffer.clear(); // Clear data but keep allocation
        let capacity = buffer.capacity();

        // Determine which bucket this buffer belongs to
        let bucket = if capacity <= 32 {
            &self.small
        } else if capacity <= 256 {
            &self.medium
        } else {
            &self.large
        };

        // Return to pool if under limit
        let mut bucket_guard = bucket.borrow_mut();
        if bucket_guard.len() < self.config.max_buffers_per_bucket {
            bucket_guard.push(buffer);
        }
        // If over limit, just drop the buffer (let it be deallocated)
    }

    /// Get a buffer from the appropriate size bucket.
    fn get_from_bucket(&self, capacity: usize) -> Option<Vec<T>> {
        let bucket = if capacity <= 32 {
            &self.small
        } else if capacity <= 256 {
            &self.medium
        } else {
            &self.large
        };

        // Find a buffer with sufficient capacity
        let mut bucket_guard = bucket.borrow_mut();
        for i in (0..bucket_guard.len()).rev() {
            if bucket_guard[i].capacity() >= capacity {
                return Some(bucket_guard.swap_remove(i));
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

        if let Ok(small) = self.small.try_borrow() {
            total += small.iter().map(|v| v.capacity() * std::mem::size_of::<T>()).sum::<usize>();
        }
        if let Ok(medium) = self.medium.try_borrow() {
            total += medium.iter().map(|v| v.capacity() * std::mem::size_of::<T>()).sum::<usize>();
        }
        if let Ok(large) = self.large.try_borrow() {
            total += large.iter().map(|v| v.capacity() * std::mem::size_of::<T>()).sum::<usize>();
        }

        total
    }

    /// Count total buffers across all buckets.
    fn count_buffers(&self) -> usize {
        let small_len = self.small.try_borrow().map(|v| v.len()).unwrap_or(0);
        let medium_len = self.medium.try_borrow().map(|v| v.len()).unwrap_or(0);
        let large_len = self.large.try_borrow().map(|v| v.len()).unwrap_or(0);
        small_len + medium_len + large_len
    }
}

impl<T> BufferPool<T> for NumericPool<T>
where
    T: Default + Clone + Send + Sync + 'static,
{
    fn acquire(&self, capacity: usize) -> PooledBuffer<T> {
        // Try to get a buffer from the pool first
        if let Some(mut buffer) = self.get_from_bucket(capacity) {
            // Ensure the buffer has the requested capacity
            if buffer.capacity() < capacity {
                buffer.reserve(capacity - buffer.capacity());
            }
            self.update_stats(true); // Hit
            PooledBuffer::new(buffer, Rc::new(RefCell::new(NumericPoolWrapper::new(self))))
        } else {
            // Allocate a new buffer
            let buffer = Vec::with_capacity(capacity);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(NumericPoolWrapper::new(self))))
        }
    }

    fn acquire_exact(&self, size: usize) -> PooledBuffer<T> {
        // For exact size, we still try to reuse but might need to resize
        if let Some(mut buffer) = self.get_from_bucket(size) {
            // Resize to exact size
            buffer.clear();
            buffer.resize_with(size, T::default);
            self.update_stats(true); // Hit
            PooledBuffer::new(buffer, Rc::new(RefCell::new(NumericPoolWrapper::new(self))))
        } else {
            // Allocate exactly what was requested
            let mut buffer = Vec::with_capacity(size);
            buffer.resize_with(size, T::default);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, Rc::new(RefCell::new(NumericPoolWrapper::new(self))))
        }
    }

    fn stats(&self) -> PoolStats {
        let mut stats = self.stats.borrow_mut();
        stats.current_buffers = self.count_buffers();
        stats.total_memory = self.calculate_memory_usage();

        // Calculate buffer size statistics
        let mut sizes = Vec::new();

        if let Ok(small) = self.small.try_borrow() {
            sizes.extend(small.iter().map(|v| v.capacity()));
        }
        if let Ok(medium) = self.medium.try_borrow() {
            sizes.extend(medium.iter().map(|v| v.capacity()));
        }
        if let Ok(large) = self.large.try_borrow() {
            sizes.extend(large.iter().map(|v| v.capacity()));
        }

        if !sizes.is_empty() {
            stats.avg_buffer_size = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
            stats.max_buffer_size = *sizes.iter().max().unwrap_or(&0);
            stats.min_buffer_size = *sizes.iter().min().unwrap_or(&0);
        }

        stats.clone()
    }

    fn clear(&self) {
        self.small.borrow_mut().clear();
        self.medium.borrow_mut().clear();
        self.large.borrow_mut().clear();
        *self.stats.borrow_mut() = PoolStats::new();
    }

    fn try_return_buffer(&self, buffer: Vec<T>) {
        self.return_buffer(buffer);
    }
}

/// Wrapper to allow the pool to be used as a trait object while maintaining
/// the ability to return buffers to the concrete pool type.
struct NumericPoolWrapper<T> {
    pool_ptr: *mut NumericPool<T>,
    _phantom: PhantomData<T>,
}

impl<T> NumericPoolWrapper<T> {
    fn new(pool: &NumericPool<T>) -> Self {
        Self {
            pool_ptr: pool as *const NumericPool<T> as *mut NumericPool<T>,
            _phantom: PhantomData,
        }
    }

    fn pool(&self) -> &NumericPool<T> {
        unsafe { &*self.pool_ptr }
    }
}

unsafe impl<T: Send + Sync> Send for NumericPoolWrapper<T> {}
unsafe impl<T: Send + Sync> Sync for NumericPoolWrapper<T> {}

impl<T> BufferPool<T> for NumericPoolWrapper<T>
where
    T: Default + Clone + Send + Sync + 'static,
{
    fn acquire(&self, capacity: usize) -> PooledBuffer<T> {
        self.pool().acquire(capacity)
    }

    fn acquire_exact(&self, size: usize) -> PooledBuffer<T> {
        self.pool().acquire_exact(size)
    }

    fn stats(&self) -> PoolStats {
        self.pool().stats()
    }

    fn clear(&self) {
        self.pool().clear()
    }

    fn try_return_buffer(&self, buffer: Vec<T>) {
        self.pool().return_buffer(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_pool_basic_operations() {
        let pool = NumericPool::<i32>::new(PoolConfig::default());

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
    fn test_numeric_pool_reuse() {
        let mut pool = NumericPool::<i32>::new(PoolConfig::default());

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
        let mut pool = NumericPool::<i32>::new(PoolConfig::default());

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
        let mut pool = NumericPool::<i32>::new(PoolConfig::default());

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
        let mut pool = NumericPool::<i32>::new(PoolConfig::default());

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
