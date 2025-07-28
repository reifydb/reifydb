// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Buffer pooling infrastructure for efficient memory reuse in query execution.
//!
//! This module provides type-safe buffer pools that dramatically reduce memory allocations
//! during query processing by reusing pre-allocated buffers.

use std::cell::RefCell;
use std::rc::Rc;

pub mod bitvec;
pub mod builder;
pub mod manager;
pub mod numeric;

pub use manager::BufferPoolManager;

/// Core trait for buffer pools that manage reusable buffers of a specific type.
pub trait BufferPool<T> {
    /// Acquire a buffer with at least the specified capacity.
    /// The returned buffer may have larger capacity for better reuse.
    fn acquire(&self, capacity: usize) -> PooledBuffer<T>;
    /// Acquire a buffer with exactly the specified size.
    /// This is useful when exact size requirements matter.
    fn acquire_exact(&self, size: usize) -> PooledBuffer<T>;
    /// Get current pool statistics for monitoring and tuning.
    fn stats(&self) -> PoolStats;
    /// Release all buffers and free memory. Used for cleanup.
    fn clear(&self);
    /// Return a buffer to the pool for reuse (if possible).
    /// Default implementation does nothing, letting specific pools override.
    fn try_return_buffer(&self, _buffer: Vec<T>) {
        // Default: do nothing, just let the buffer be dropped
    }
}

/// A buffer that automatically returns to its pool when dropped.
/// Provides Vec-like interface while maintaining pool integration.
pub struct PooledBuffer<T> {
    data: Vec<T>,
    pool: Option<Rc<RefCell<dyn BufferPool<T>>>>,
}

impl<T> PooledBuffer<T> {
    /// Create a new pooled buffer with the given data and pool reference.
    pub(crate) fn new(data: Vec<T>, pool: Rc<RefCell<dyn BufferPool<T>>>) -> Self {
        Self { data, pool: Some(pool) }
    }

    /// Create a non-pooled buffer for cases where pooling is not available.
    pub fn unpooled(data: Vec<T>) -> Self {
        Self { pool: None, data }
    }

    /// Get a reference to the underlying data as a slice.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Get a mutable reference to the underlying data as a slice.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Get the length of the buffer.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Push a value to the buffer.
    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    /// Extend the buffer with an iterator.
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.data.extend(iter);
    }

    /// Clear the buffer, keeping the allocation.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Reserve capacity for at least `additional` more elements.
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Get a reference to the element at the given index.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    /// Get a mutable reference to the element at the given index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)
    }

    /// Convert the pooled buffer into a regular Vec, consuming the buffer.
    pub fn into_vec(mut self) -> Vec<T> {
        // Prevent the drop implementation from running
        self.pool = None;
        std::mem::take(&mut self.data)
    }

    /// Get the raw pointer to the buffer data.
    pub fn as_ptr(&self) -> *const T {
        self.data.as_ptr()
    }

    /// Get a mutable raw pointer to the buffer data.
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.data.as_mut_ptr()
    }
}

impl<T> Drop for PooledBuffer<T> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            // Return the buffer to the pool for reuse
            self.return_to_pool(pool);
        }
    }
}

impl<T> PooledBuffer<T> {
    fn return_to_pool(&mut self, pool: Rc<RefCell<dyn BufferPool<T>>>) {
        // Use the new try_return_buffer method
        let data = std::mem::take(&mut self.data);
        pool.borrow_mut().try_return_buffer(data);
    }
}

impl<T> std::ops::Deref for PooledBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> std::ops::DerefMut for PooledBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> std::ops::Index<usize> for PooledBuffer<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> std::ops::IndexMut<usize> for PooledBuffer<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

/// Statistics about buffer pool usage for monitoring and optimization.
#[derive(Debug, Clone)]
pub struct PoolStats {
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

impl PoolStats {
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
    pub fn update_hit_rate(&mut self) {
        let total = self.hits + self.misses;
        self.hit_rate = if total > 0 { self.hits as f64 / total as f64 } else { 0.0 };
    }
}

impl Default for PoolStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for buffer pools.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum memory usage in megabytes across all pools.
    pub max_memory_mb: usize,

    /// Maximum number of buffers to keep per size bucket.
    pub max_buffers_per_bucket: usize,

    /// Memory usage threshold (0.0-1.0) above which cleanup is triggered.
    pub trim_threshold: f64,

    /// Size buckets for efficient buffer reuse.
    pub size_buckets: Vec<usize>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_buffers_per_bucket: 64,
            trim_threshold: 0.8,
            size_buckets: vec![8, 32, 128, 256, 512, 1024],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooled_buffer_basic_operations() {
        let data = vec![1, 2, 3, 4, 5];
        let mut buffer = PooledBuffer::unpooled(data);

        assert_eq!(buffer.len(), 5);
        assert_eq!(buffer[0], 1);
        assert_eq!(buffer.as_slice(), &[1, 2, 3, 4, 5]);

        buffer.push(6);
        assert_eq!(buffer.len(), 6);
        assert_eq!(buffer[5], 6);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_pool_stats() {
        let mut stats = PoolStats::new();
        assert_eq!(stats.hit_rate, 0.0);

        stats.hits = 8;
        stats.misses = 2;
        stats.update_hit_rate();
        assert_eq!(stats.hit_rate, 0.8);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_memory_mb, 512);
        assert_eq!(config.max_buffers_per_bucket, 64);
        assert_eq!(config.trim_threshold, 0.8);
        assert!(!config.size_buckets.is_empty());
    }
}
