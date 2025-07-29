// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Container pooling infrastructure for efficient memory management during expression evaluation.
//!
//! This module provides pooling for all container types to avoid frequent allocations and
//! deallocations during columnar operations. Each container type has its own pool that
//! manages reusable instances.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::frame::column::container::*;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::value::{Date, DateTime, Interval, Time};
use crate::value::{IsNumber, IsTemporal, IsUuid};

/// Core trait for container pooling operations
pub trait Pool<C> {
    /// Get a container with at least the specified capacity
    fn acquire(&self, capacity: usize) -> C;

    /// Return a container to the pool for reuse
    fn release(&self, container: C);

    /// Clear all pooled containers
    fn clear(&self);

    /// Get statistics about pool usage
    fn stats(&self) -> PoolStats;
}

/// Statistics about pool usage
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub available: usize,
    pub total_acquired: usize,
    pub total_released: usize,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self { available: 0, total_acquired: 0, total_released: 0 }
    }
}

/// Generic pool implementation for any container type
pub struct Pools<C> {
    pools: Arc<Mutex<HashMap<usize, Vec<C>>>>,
    stats: Arc<Mutex<PoolStats>>,
    max_pool_size: usize,
}

impl<C> Pools<C> {
    pub fn new(max_pool_size: usize) -> Self {
        let result = Self {
            pools: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
            max_pool_size,
        };
        result
    }

    /// Create a new container with the specified capacity
    fn create_new(&self, capacity: usize) -> C
    where
        C: ContainerCapacity,
    {
        C::with_capacity(capacity)
    }

    /// Get the capacity bucket for a given capacity (rounds up to nearest power of 2)
    fn capacity_bucket(capacity: usize) -> usize {
        if capacity == 0 {
            return 8; // minimum bucket size
        }
        capacity.next_power_of_two().max(8)
    }
}

/// Trait for containers that can be created with a specific capacity
pub trait ContainerCapacity {
    fn with_capacity(capacity: usize) -> Self;
    fn clear(&mut self);
    fn capacity(&self) -> usize;
}

// Implement ContainerCapacity for all our container types
impl ContainerCapacity for BoolContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl<T> ContainerCapacity for NumberContainer<T>
where
    T: IsNumber + Clone + std::fmt::Debug + Default,
{
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl ContainerCapacity for StringContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl<T> ContainerCapacity for TemporalContainer<T>
where
    T: IsTemporal + Clone + std::fmt::Debug + Default,
{
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl<T> ContainerCapacity for UuidContainer<T>
where
    T: IsUuid + Clone + std::fmt::Debug + Default,
{
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl ContainerCapacity for BlobContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl ContainerCapacity for RowIdContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl ContainerCapacity for UndefinedContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn clear(&mut self) {
        // Clear content but preserve capacity
        let capacity = self.capacity();
        *self = Self::with_capacity(capacity);
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl<C> Pool<C> for Pools<C>
where
    C: ContainerCapacity,
{
    fn acquire(&self, capacity: usize) -> C {
        let bucket = Self::capacity_bucket(capacity);

        // Try to get from pool first
        if let Ok(mut pools) = self.pools.lock() {
            if let Some(pool) = pools.get_mut(&bucket) {
                if let Some(mut container) = pool.pop() {
                    container.clear();

                    // Update stats
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.total_acquired += 1;
                    }

                    return container;
                }
            }
        }

        // Create new container if pool is empty
        let container = self.create_new(bucket);

        // Update stats
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_acquired += 1;
        }

        container
    }

    fn release(&self, container: C) {
        let capacity = container.capacity();
        let bucket = Self::capacity_bucket(capacity);

        if let Ok(mut pools) = self.pools.lock() {
            let pool = pools.entry(bucket).or_insert_with(Vec::new);

            // Only keep containers if we haven't exceeded max pool size
            if pool.len() < self.max_pool_size {
                pool.push(container);
            }
        }

        // Update stats
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_released += 1;
        }
    }

    fn clear(&self) {
        if let Ok(mut pools) = self.pools.lock() {
            pools.clear();
        }

        if let Ok(mut stats) = self.stats.lock() {
            *stats = PoolStats::default();
        }
    }

    fn stats(&self) -> PoolStats {
        if let Ok(pools) = self.pools.lock() {
            let available = pools.values().map(|pool| pool.len()).sum();

            if let Ok(stats) = self.stats.lock() {
                return PoolStats {
                    available,
                    total_acquired: stats.total_acquired,
                    total_released: stats.total_released,
                };
            }
        }

        PoolStats::default()
    }
}

/// Global container pool manager
pub struct ContainerPools {
    bool_pool: Pools<BoolContainer>,
    string_pool: Pools<StringContainer>,
    blob_pool: Pools<BlobContainer>,
    row_id_pool: Pools<RowIdContainer>,
    undefined_pool: Pools<UndefinedContainer>,

    // Numeric pools for common types
    i32_pool: Pools<NumberContainer<i32>>,
    i64_pool: Pools<NumberContainer<i64>>,
    f32_pool: Pools<NumberContainer<f32>>,
    f64_pool: Pools<NumberContainer<f64>>,

    // Temporal pools
    date_pool: Pools<TemporalContainer<Date>>,
    datetime_pool: Pools<TemporalContainer<DateTime>>,
    time_pool: Pools<TemporalContainer<Time>>,
    interval_pool: Pools<TemporalContainer<Interval>>,

    // UUID pools
    uuid4_pool: Pools<UuidContainer<Uuid4>>,
    uuid7_pool: Pools<UuidContainer<Uuid7>>,
}

impl ContainerPools {
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            bool_pool: Pools::new(max_pool_size),
            string_pool: Pools::new(max_pool_size),
            blob_pool: Pools::new(max_pool_size),
            row_id_pool: Pools::new(max_pool_size),
            undefined_pool: Pools::new(max_pool_size),

            i32_pool: Pools::new(max_pool_size),
            i64_pool: Pools::new(max_pool_size),
            f32_pool: Pools::new(max_pool_size),
            f64_pool: Pools::new(max_pool_size),

            date_pool: Pools::new(max_pool_size),
            datetime_pool: Pools::new(max_pool_size),
            time_pool: Pools::new(max_pool_size),
            interval_pool: Pools::new(max_pool_size),

            uuid4_pool: Pools::new(max_pool_size),
            uuid7_pool: Pools::new(max_pool_size),
        }
    }

    // Accessors for each pool type
    pub fn bool_pool(&self) -> &Pools<BoolContainer> {
        &self.bool_pool
    }
    pub fn string_pool(&self) -> &Pools<StringContainer> {
        &self.string_pool
    }
    pub fn blob_pool(&self) -> &Pools<BlobContainer> {
        &self.blob_pool
    }
    pub fn row_id_pool(&self) -> &Pools<RowIdContainer> {
        &self.row_id_pool
    }
    pub fn undefined_pool(&self) -> &Pools<UndefinedContainer> {
        &self.undefined_pool
    }

    pub fn i32_pool(&self) -> &Pools<NumberContainer<i32>> {
        &self.i32_pool
    }
    pub fn i64_pool(&self) -> &Pools<NumberContainer<i64>> {
        &self.i64_pool
    }
    pub fn f32_pool(&self) -> &Pools<NumberContainer<f32>> {
        &self.f32_pool
    }
    pub fn f64_pool(&self) -> &Pools<NumberContainer<f64>> {
        &self.f64_pool
    }

    pub fn date_pool(&self) -> &Pools<TemporalContainer<Date>> {
        &self.date_pool
    }
    pub fn datetime_pool(&self) -> &Pools<TemporalContainer<DateTime>> {
        &self.datetime_pool
    }
    pub fn time_pool(&self) -> &Pools<TemporalContainer<Time>> {
        &self.time_pool
    }
    pub fn interval_pool(&self) -> &Pools<TemporalContainer<Interval>> {
        &self.interval_pool
    }

    pub fn uuid4_pool(&self) -> &Pools<UuidContainer<Uuid4>> {
        &self.uuid4_pool
    }
    pub fn uuid7_pool(&self) -> &Pools<UuidContainer<Uuid7>> {
        &self.uuid7_pool
    }

    /// Clear all pools
    pub fn clear_all(&self) {
        self.bool_pool.clear();
        self.string_pool.clear();
        self.blob_pool.clear();
        self.row_id_pool.clear();
        self.undefined_pool.clear();

        self.i32_pool.clear();
        self.i64_pool.clear();
        self.f32_pool.clear();
        self.f64_pool.clear();

        self.date_pool.clear();
        self.datetime_pool.clear();
        self.time_pool.clear();
        self.interval_pool.clear();

        self.uuid4_pool.clear();
        self.uuid7_pool.clear();
    }

    /// Get statistics for all pools
    pub fn all_stats(&self) -> HashMap<String, PoolStats> {
        let mut stats = HashMap::new();

        stats.insert("bool".to_string(), self.bool_pool.stats());
        stats.insert("string".to_string(), self.string_pool.stats());
        stats.insert("blob".to_string(), self.blob_pool.stats());
        stats.insert("row_id".to_string(), self.row_id_pool.stats());
        stats.insert("undefined".to_string(), self.undefined_pool.stats());

        stats.insert("i32".to_string(), self.i32_pool.stats());
        stats.insert("i64".to_string(), self.i64_pool.stats());
        stats.insert("f32".to_string(), self.f32_pool.stats());
        stats.insert("f64".to_string(), self.f64_pool.stats());

        stats.insert("date".to_string(), self.date_pool.stats());
        stats.insert("datetime".to_string(), self.datetime_pool.stats());
        stats.insert("time".to_string(), self.time_pool.stats());
        stats.insert("interval".to_string(), self.interval_pool.stats());

        stats.insert("uuid4".to_string(), self.uuid4_pool.stats());
        stats.insert("uuid7".to_string(), self.uuid7_pool.stats());

        stats
    }
}

impl Default for ContainerPools {
    fn default() -> Self {
        Self::new(16) // Default max pool size of 16 containers per bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacity_bucket() {
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(0), 8);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(1), 8);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(8), 8);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(9), 16);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(16), 16);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(17), 32);
        assert_eq!(Pools::<BoolContainer>::capacity_bucket(100), 128);
    }

    #[test]
    fn test_bool_container_pool() {
        let pools = Pools::<BoolContainer>::new(4);

        // Acquire a container
        let container1 = pools.acquire(10);
        assert!(container1.capacity() >= 10);

        // Release it back to pool
        pools.release(container1);

        // Stats should reflect the operation
        let stats = pools.stats();
        assert_eq!(stats.total_acquired, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.available, 1);

        // Acquire again should reuse the container
        let container2 = pools.acquire(10);
        let stats2 = pools.stats();
        assert_eq!(stats2.total_acquired, 2);
        assert_eq!(stats2.available, 0);

        pools.release(container2);
    }

    #[test]
    fn test_string_container_pool() {
        let pools = Pools::<StringContainer>::new(4);

        let container = pools.acquire(20);
        assert!(container.capacity() >= 20);

        pools.release(container);

        let stats = pools.stats();
        assert_eq!(stats.total_acquired, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.available, 1);
    }

    #[test]
    fn test_number_container_pool() {
        let pools = Pools::<NumberContainer<i32>>::new(4);

        let container = pools.acquire(50);
        assert!(container.capacity() >= 50);

        pools.release(container);

        let stats = pools.stats();
        assert_eq!(stats.total_acquired, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.available, 1);
    }

    #[test]
    fn test_pool_max_size() {
        let pools = Pools::<BoolContainer>::new(2);

        // Fill the pool beyond its max size
        let c1 = pools.acquire(10);
        let c2 = pools.acquire(10);
        let c3 = pools.acquire(10);

        pools.release(c1);
        pools.release(c2);
        pools.release(c3); // This should be discarded due to max size

        let stats = pools.stats();
        assert_eq!(stats.available, 2); // Only 2 should be kept due to max_pool_size
    }

    #[test]
    fn test_container_pools() {
        let pools = ContainerPools::new(4);

        // Test different pool types
        let bool_container = pools.bool_pool().acquire(10);
        let string_container = pools.string_pool().acquire(20);
        let i32_container = pools.i32_pool().acquire(30);

        pools.bool_pool().release(bool_container);
        pools.string_pool().release(string_container);
        pools.i32_pool().release(i32_container);

        let all_stats = pools.all_stats();
        assert_eq!(all_stats["bool"].available, 1);
        assert_eq!(all_stats["string"].available, 1);
        assert_eq!(all_stats["i32"].available, 1);
    }

    #[test]
    fn test_clear_pools() {
        let pools = ContainerPools::new(4);

        let container = pools.bool_pool().acquire(10);
        pools.bool_pool().release(container);

        assert_eq!(pools.bool_pool().stats().available, 1);

        pools.clear_all();

        assert_eq!(pools.bool_pool().stats().available, 0);
    }
}
