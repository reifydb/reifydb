// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Buffer pooling infrastructure for efficient memory reuse in query execution.
//!
//! This module provides type-safe buffer pools that dramatically reduce memory allocations
//! during query processing by reusing pre-allocated buffers.

use std::cell::RefCell;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::rc::Rc;

pub mod container;
pub mod config;
pub mod impl_bitvec;
mod impl_bool;
mod impl_number;
pub mod impl_numeric;
pub mod pool;
pub mod pools;
pub mod statistics;

pub use config::PoolConfig;
pub use impl_bool::BooleanPool;
pub use impl_number::NumberPool;
pub use pools::BufferedPools;
pub use statistics::PoolStatistics;

/// Size thresholds for different buckets.
pub(crate) const POOL_SIZE_THRESHOLDS: [usize; 6] = [8, 32, 128, 256, 512, 1024];

/// Core trait for buffer pools that manage reusable buffers of a specific type.
pub trait BufferedPool<T> {
    /// Acquire a buffer with at least the specified capacity.
    /// The returned buffer may have larger capacity for better reuse.
    fn acquire(&self, capacity: usize) -> PooledBuffer<T>;
    /// Get current pool statistics for monitoring and tuning.
    fn stats(&self) -> PoolStatistics;
    /// Release all buffers and free memory. Used for cleanup.
    fn clear(&self);
    /// Return a buffer to the pool for reuse (if possible).
    fn release(&self, buffer: Vec<T>);
}

/// A buffer that automatically returns to its pool when dropped.
/// Provides Vec-like interface while maintaining pool integration.
pub struct PooledBuffer<T> {
    data: Vec<T>,
    pool: Option<Rc<RefCell<dyn BufferedPool<T>>>>,
}

impl<T> PooledBuffer<T> {
    /// Create a new pooled buffer with the given data and pool reference.
    pub(crate) fn new(data: Vec<T>, pool: Rc<RefCell<dyn BufferedPool<T>>>) -> Self {
        Self { data, pool: Some(pool) }
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
    fn return_to_pool(&mut self, pool: Rc<RefCell<dyn BufferedPool<T>>>) {
        // Use the new try_return_buffer method
        let data = std::mem::take(&mut self.data);
        pool.borrow_mut().release(data);
    }
}

impl<T> Deref for PooledBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for PooledBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> Index<usize> for PooledBuffer<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for PooledBuffer<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.data[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_stats() {
        let mut stats = PoolStatistics::new();
        assert_eq!(stats.hit_rate, 0.0);

        stats.hits = 8;
        stats.misses = 2;
        stats.update_hit_rate();
        assert_eq!(stats.hit_rate, 0.8);
    }
}
