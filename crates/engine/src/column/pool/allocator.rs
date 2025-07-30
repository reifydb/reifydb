// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::pool::capacity::ContainerCapacity;
use crate::column::pool::stats::PoolStats;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Core trait for container pooling operations
pub trait PoolAllocator<C> {
    /// Get a container with at least the specified capacity
    fn acquire(&self, capacity: usize) -> C;

    /// Return a container to the pool for reuse
    fn release(&self, container: C);

    /// Clear all pooled containers
    fn clear(&self);

    /// Get statistics about pool usage
    fn stats(&self) -> PoolStats;
}

/// Generic pool implementation for any container type
pub struct StdPoolAllocator<C> {
    pools: Arc<Mutex<HashMap<usize, Vec<C>>>>,
    stats: Arc<Mutex<PoolStats>>,
    max_pool_size: usize,
}

impl<C> StdPoolAllocator<C> {
    pub(crate) fn new(max_pool_size: usize) -> Self {
        let result = Self {
            pools: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
            max_pool_size,
        };
        result
    }

    /// Create a new container with the specified capacity
    pub(crate) fn create_new(&self, capacity: usize) -> C
    where
        C: ContainerCapacity,
    {
        C::with_capacity(capacity)
    }

    /// Get the capacity bucket for a given capacity (rounds up to nearest power of 2)
    pub(crate) fn capacity_bucket(capacity: usize) -> usize {
        if capacity == 0 {
            return 8; // minimum bucket size
        }
        capacity.next_power_of_two().max(8)
    }
}

impl<C> PoolAllocator<C> for StdPoolAllocator<C>
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

#[cfg(test)]
mod tests {
    use crate::column::container::{BoolContainer, NumberContainer, StringContainer};
    use crate::column::pool::allocator::{PoolAllocator, StdPoolAllocator};

    #[test]
    fn test_allocate_bool() {
        let pools = StdPoolAllocator::<BoolContainer>::new(4);

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
    fn test_allocate_string() {
        let pools = StdPoolAllocator::<StringContainer>::new(4);

        let container = pools.acquire(20);
        assert!(container.capacity() >= 20);

        pools.release(container);

        let stats = pools.stats();
        assert_eq!(stats.total_acquired, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.available, 1);
    }

    #[test]
    fn test_allocate_number() {
        let pools = StdPoolAllocator::<NumberContainer<i32>>::new(4);

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
        let pools = StdPoolAllocator::<BoolContainer>::new(2);

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
    fn test_capacity_bucket() {
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(0), 8);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(1), 8);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(8), 8);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(9), 16);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(16), 16);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(17), 32);
        assert_eq!(StdPoolAllocator::<BoolContainer>::capacity_bucket(100), 128);
    }
}
