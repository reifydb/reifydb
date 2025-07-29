// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Complete buffer pool manager that coordinates all buffer pools.

use super::{PoolConfig, impl_bitvec::BitVecPool, impl_numeric::NumericPool};
use std::ops::Deref;
use std::rc::Rc;

/// BufferedPools give access to all pools
#[derive(Debug)]
pub struct BufferedPools(Rc<BufferedPoolsInner>);

/// Inner structure containing all the buffer pools.
#[derive(Debug)]
pub struct BufferedPoolsInner {
    pub(crate) bool_pool: BitVecPool,

    pub(crate) i8_pool: NumericPool<i8>,
    pub(crate) i16_pool: NumericPool<i16>,
    pub(crate) i32_pool: NumericPool<i32>,
    pub(crate) i64_pool: NumericPool<i64>,
    pub(crate) i128_pool: NumericPool<i128>,
    pub(crate) u8_pool: NumericPool<u8>,
    pub(crate) u16_pool: NumericPool<u16>,
    pub(crate) u32_pool: NumericPool<u32>,
    pub(crate) u64_pool: NumericPool<u64>,
    pub(crate) u128_pool: NumericPool<u128>,
    pub(crate) f32_pool: NumericPool<f32>,
    pub(crate) f64_pool: NumericPool<f64>,
}

impl BufferedPools {
    /// Create a new buffer pool manager with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self(Rc::new(BufferedPoolsInner {
            bool_pool: BitVecPool::new(config.clone()),
            i8_pool: NumericPool::new(config.clone()),
            i16_pool: NumericPool::new(config.clone()),
            i32_pool: NumericPool::new(config.clone()),
            i64_pool: NumericPool::new(config.clone()),
            i128_pool: NumericPool::new(config.clone()),
            u8_pool: NumericPool::new(config.clone()),
            u16_pool: NumericPool::new(config.clone()),
            u32_pool: NumericPool::new(config.clone()),
            u64_pool: NumericPool::new(config.clone()),
            u128_pool: NumericPool::new(config.clone()),
            f32_pool: NumericPool::new(config.clone()),
            f64_pool: NumericPool::new(config.clone()),
        }))
    }
}

impl Deref for BufferedPools {
    type Target = BufferedPoolsInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for BufferedPools {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl Default for BufferedPools {
    fn default() -> Self {
        Self::new(PoolConfig::default())
    }
}
