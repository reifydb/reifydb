// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Container pooling infrastructure for efficient memory management during expression evaluation.
//!
//! This module provides pooling for all container types to avoid frequent allocations and
//! deallocations during columnar operations. Each container type has its own pool that
//! manages reusable instances.

mod allocator;
mod capacity;
mod guard;
mod stats;
pub mod thread_local;
pub mod config;
pub mod scoped;
pub mod lazy;
#[cfg(test)]
pub mod testing;

pub use guard::PooledGuard;
pub use config::{PoolConfig, init_thread_pools, init_default_thread_pools, init_test_pools};
pub use scoped::{ScopedPools, with_scoped_pools, with_default_pools, with_test_pools};
pub use thread_local::{thread_pools, get_thread_pools, has_thread_pools};
pub use lazy::{get_or_init_pools, ensure_thread_pools, thread_pools_lazy};

use crate::columnar::pool::allocator::{PoolAllocator, StdPoolAllocator};
use crate::columnar::pool::stats::PoolStats;
use reifydb_core::value::container::*;
use reifydb_core::value::uuid::{Uuid4, Uuid7};
use reifydb_core::value::{Date, DateTime, Interval, Time};
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone)]
pub struct Pools(Rc<PoolsInner>);

impl Deref for Pools {
    type Target = PoolsInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct PoolsInner {
    bool_pool: StdPoolAllocator<BoolContainer>,
    string_pool: StdPoolAllocator<StringContainer>,
    blob_pool: StdPoolAllocator<BlobContainer>,
    row_id_pool: StdPoolAllocator<RowIdContainer>,
    undefined_pool: StdPoolAllocator<UndefinedContainer>,

    // Numeric pools for all types
    i8_pool: StdPoolAllocator<NumberContainer<i8>>,
    i16_pool: StdPoolAllocator<NumberContainer<i16>>,
    i32_pool: StdPoolAllocator<NumberContainer<i32>>,
    i64_pool: StdPoolAllocator<NumberContainer<i64>>,
    i128_pool: StdPoolAllocator<NumberContainer<i128>>,
    u8_pool: StdPoolAllocator<NumberContainer<u8>>,
    u16_pool: StdPoolAllocator<NumberContainer<u16>>,
    u32_pool: StdPoolAllocator<NumberContainer<u32>>,
    u64_pool: StdPoolAllocator<NumberContainer<u64>>,
    u128_pool: StdPoolAllocator<NumberContainer<u128>>,
    f32_pool: StdPoolAllocator<NumberContainer<f32>>,
    f64_pool: StdPoolAllocator<NumberContainer<f64>>,

    // Temporal pools
    date_pool: StdPoolAllocator<TemporalContainer<Date>>,
    datetime_pool: StdPoolAllocator<TemporalContainer<DateTime>>,
    time_pool: StdPoolAllocator<TemporalContainer<Time>>,
    interval_pool: StdPoolAllocator<TemporalContainer<Interval>>,

    // UUID pools
    uuid4_pool: StdPoolAllocator<UuidContainer<Uuid4>>,
    uuid7_pool: StdPoolAllocator<UuidContainer<Uuid7>>,
}

impl Default for Pools {
    fn default() -> Self {
        Self::new(16) // Default max pool size of 16 containers per bucket
    }
}

impl Pools {
    pub fn new(max_pool_size: usize) -> Self {
        Self(Rc::new(PoolsInner {
            bool_pool: StdPoolAllocator::new(max_pool_size),
            string_pool: StdPoolAllocator::new(max_pool_size),
            blob_pool: StdPoolAllocator::new(max_pool_size),
            row_id_pool: StdPoolAllocator::new(max_pool_size),
            undefined_pool: StdPoolAllocator::new(max_pool_size),

            i8_pool: StdPoolAllocator::new(max_pool_size),
            i16_pool: StdPoolAllocator::new(max_pool_size),
            i32_pool: StdPoolAllocator::new(max_pool_size),
            i64_pool: StdPoolAllocator::new(max_pool_size),
            i128_pool: StdPoolAllocator::new(max_pool_size),
            u8_pool: StdPoolAllocator::new(max_pool_size),
            u16_pool: StdPoolAllocator::new(max_pool_size),
            u32_pool: StdPoolAllocator::new(max_pool_size),
            u64_pool: StdPoolAllocator::new(max_pool_size),
            u128_pool: StdPoolAllocator::new(max_pool_size),
            f32_pool: StdPoolAllocator::new(max_pool_size),
            f64_pool: StdPoolAllocator::new(max_pool_size),

            date_pool: StdPoolAllocator::new(max_pool_size),
            datetime_pool: StdPoolAllocator::new(max_pool_size),
            time_pool: StdPoolAllocator::new(max_pool_size),
            interval_pool: StdPoolAllocator::new(max_pool_size),

            uuid4_pool: StdPoolAllocator::new(max_pool_size),
            uuid7_pool: StdPoolAllocator::new(max_pool_size),
        }))
    }

    // Accessors for each pool type
    pub fn bool_pool(&self) -> &StdPoolAllocator<BoolContainer> {
        &self.bool_pool
    }
    pub fn string_pool(&self) -> &StdPoolAllocator<StringContainer> {
        &self.string_pool
    }
    pub fn blob_pool(&self) -> &StdPoolAllocator<BlobContainer> {
        &self.blob_pool
    }
    pub fn row_id_pool(&self) -> &StdPoolAllocator<RowIdContainer> {
        &self.row_id_pool
    }
    pub fn undefined_pool(&self) -> &StdPoolAllocator<UndefinedContainer> {
        &self.undefined_pool
    }

    pub fn i8_pool(&self) -> &StdPoolAllocator<NumberContainer<i8>> {
        &self.i8_pool
    }
    pub fn i16_pool(&self) -> &StdPoolAllocator<NumberContainer<i16>> {
        &self.i16_pool
    }
    pub fn i32_pool(&self) -> &StdPoolAllocator<NumberContainer<i32>> {
        &self.i32_pool
    }
    pub fn i64_pool(&self) -> &StdPoolAllocator<NumberContainer<i64>> {
        &self.i64_pool
    }
    pub fn i128_pool(&self) -> &StdPoolAllocator<NumberContainer<i128>> {
        &self.i128_pool
    }
    pub fn u8_pool(&self) -> &StdPoolAllocator<NumberContainer<u8>> {
        &self.u8_pool
    }
    pub fn u16_pool(&self) -> &StdPoolAllocator<NumberContainer<u16>> {
        &self.u16_pool
    }
    pub fn u32_pool(&self) -> &StdPoolAllocator<NumberContainer<u32>> {
        &self.u32_pool
    }
    pub fn u64_pool(&self) -> &StdPoolAllocator<NumberContainer<u64>> {
        &self.u64_pool
    }
    pub fn u128_pool(&self) -> &StdPoolAllocator<NumberContainer<u128>> {
        &self.u128_pool
    }
    pub fn f32_pool(&self) -> &StdPoolAllocator<NumberContainer<f32>> {
        &self.f32_pool
    }
    pub fn f64_pool(&self) -> &StdPoolAllocator<NumberContainer<f64>> {
        &self.f64_pool
    }

    pub fn date_pool(&self) -> &StdPoolAllocator<TemporalContainer<Date>> {
        &self.date_pool
    }
    pub fn datetime_pool(&self) -> &StdPoolAllocator<TemporalContainer<DateTime>> {
        &self.datetime_pool
    }
    pub fn time_pool(&self) -> &StdPoolAllocator<TemporalContainer<Time>> {
        &self.time_pool
    }
    pub fn interval_pool(&self) -> &StdPoolAllocator<TemporalContainer<Interval>> {
        &self.interval_pool
    }

    pub fn uuid4_pool(&self) -> &StdPoolAllocator<UuidContainer<Uuid4>> {
        &self.uuid4_pool
    }
    pub fn uuid7_pool(&self) -> &StdPoolAllocator<UuidContainer<Uuid7>> {
        &self.uuid7_pool
    }

    /// Clear all pools
    pub fn clear_all(&self) {
        self.bool_pool.clear();
        self.string_pool.clear();
        self.blob_pool.clear();
        self.row_id_pool.clear();
        self.undefined_pool.clear();

        self.i8_pool.clear();
        self.i16_pool.clear();
        self.i32_pool.clear();
        self.i64_pool.clear();
        self.i128_pool.clear();
        self.u8_pool.clear();
        self.u16_pool.clear();
        self.u32_pool.clear();
        self.u64_pool.clear();
        self.u128_pool.clear();
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

        stats.insert("i8".to_string(), self.i8_pool.stats());
        stats.insert("i16".to_string(), self.i16_pool.stats());
        stats.insert("i32".to_string(), self.i32_pool.stats());
        stats.insert("i64".to_string(), self.i64_pool.stats());
        stats.insert("i128".to_string(), self.i128_pool.stats());
        stats.insert("u8".to_string(), self.u8_pool.stats());
        stats.insert("u16".to_string(), self.u16_pool.stats());
        stats.insert("u32".to_string(), self.u32_pool.stats());
        stats.insert("u64".to_string(), self.u64_pool.stats());
        stats.insert("u128".to_string(), self.u128_pool.stats());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_pools() {
        let pools = Pools::new(4);

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
        let pools = Pools::new(4);

        let container = pools.bool_pool().acquire(10);
        pools.bool_pool().release(container);

        assert_eq!(pools.bool_pool().stats().available, 1);

        pools.clear_all();

        assert_eq!(pools.bool_pool().stats().available, 0);
    }
}
