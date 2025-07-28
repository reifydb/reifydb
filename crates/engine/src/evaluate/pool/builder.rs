// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pooled column builder that uses buffer pools for efficient memory allocation.

use super::{BufferPoolManager, BufferPool};
use reifydb_core::frame::ColumnValues;
use reifydb_core::{BitVec, CowVec, Type};
use std::sync::Arc;

/// Builder for ColumnValues that uses buffer pools for efficient memory allocation.
pub struct PooledColumnBuilder<'a> {
    pool: &'a BufferPoolManager,
    target_type: Type,
    capacity: usize,
}

impl<'a> PooledColumnBuilder<'a> {
    /// Create a new pooled column builder with the specified type and capacity.
    pub fn with_capacity(
        pool: &'a BufferPoolManager,
        target_type: Type,
        capacity: usize,
    ) -> Self {
        Self {
            pool,
            target_type,
            capacity,
        }
    }

    /// Build the ColumnValues using pooled buffers where possible.
    pub fn build(self) -> ColumnValues {
        match self.target_type {
            Type::Bool => {
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                let values_vec = vec![false; 0]; // Empty vec, will be populated by caller
                ColumnValues::Bool(CowVec::new(values_vec), bitvec)
            }
            Type::Int1 => {
                let buffer = self.pool.i8_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Int1(CowVec::new(vec), bitvec)
            }
            Type::Int2 => {
                let buffer = self.pool.i16_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Int2(CowVec::new(vec), bitvec)
            }
            Type::Int4 => {
                let buffer = self.pool.i32_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Int4(CowVec::new(vec), bitvec)
            }
            Type::Int8 => {
                let buffer = self.pool.i64_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Int8(CowVec::new(vec), bitvec)
            }
            Type::Int16 => {
                let buffer = self.pool.i128_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Int16(CowVec::new(vec), bitvec)
            }
            Type::Uint1 => {
                let buffer = self.pool.u8_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Uint1(CowVec::new(vec), bitvec)
            }
            Type::Uint2 => {
                let buffer = self.pool.u16_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Uint2(CowVec::new(vec), bitvec)
            }
            Type::Uint4 => {
                let buffer = self.pool.u32_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Uint4(CowVec::new(vec), bitvec)
            }
            Type::Uint8 => {
                let buffer = self.pool.u64_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Uint8(CowVec::new(vec), bitvec)
            }
            Type::Uint16 => {
                let buffer = self.pool.u128_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Uint16(CowVec::new(vec), bitvec)
            }
            Type::Float4 => {
                let buffer = self.pool.f32_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Float4(CowVec::new(vec), bitvec)
            }
            Type::Float8 => {
                let buffer = self.pool.f64_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Float8(CowVec::new(vec), bitvec)
            }
            Type::Utf8 => {
                let buffer = self.pool.utf8_pool.acquire(self.capacity);
                let vec = buffer.into_vec();
                let bitvec = self.pool.bool_pool.acquire_bitvec(self.capacity);
                ColumnValues::Utf8(CowVec::new(vec), bitvec)
            }
            // For temporal types, we fall back to regular allocation for now
            // TODO: Add specialized pools for Date, DateTime, Time, Interval
            Type::Date => ColumnValues::date_with_capacity(self.capacity),
            Type::DateTime => ColumnValues::datetime_with_capacity(self.capacity),
            Type::Time => ColumnValues::time_with_capacity(self.capacity),
            Type::Interval => ColumnValues::interval_with_capacity(self.capacity),
            Type::RowId => ColumnValues::row_id_with_capacity(self.capacity),
            Type::Uuid4 => ColumnValues::uuid4_with_capacity(self.capacity),
            Type::Uuid7 => ColumnValues::uuid7_with_capacity(self.capacity),
            Type::Undefined => ColumnValues::undefined(self.capacity),
        }
    }
}

/// Extension trait to add pooled building capabilities to ColumnValues.
pub trait ColumnValuesExt {
    /// Create ColumnValues using buffer pools for efficient memory allocation.
    fn with_pooled_capacity(
        target_type: Type,
        capacity: usize,
        pool: &BufferPoolManager,
    ) -> ColumnValues;
}

impl ColumnValuesExt for ColumnValues {
    fn with_pooled_capacity(
        target_type: Type,
        capacity: usize,
        pool: &BufferPoolManager,
    ) -> ColumnValues {
        PooledColumnBuilder::with_capacity(pool, target_type, capacity).build()
    }
}

/// Helper for building columns with pre-allocated pooled buffers.
pub struct PooledVectorBuilder<T> {
    buffer: Vec<T>,
    valid: BitVec,
    pool_manager: Arc<BufferPoolManager>,
}

impl<T> PooledVectorBuilder<T>
where
    T: Default + Clone + Send + Sync + 'static + PartialEq,
{
    /// Create a new pooled vector builder.
    pub fn new(
        capacity: usize,
        pool_manager: Arc<BufferPoolManager>,
        pool_type: PoolType,
    ) -> Self {
        let buffer = match pool_type {
            PoolType::I8 => {
                // This is a bit tricky with generics, so we'll handle specific types
                // For now, we'll use direct allocation and improve this later
                Vec::with_capacity(capacity)
            }
            _ => Vec::with_capacity(capacity),
        };

        let valid = pool_manager.bool_pool.acquire_bitvec(capacity);

        Self {
            buffer,
            valid,
            pool_manager,
        }
    }

    /// Push a value to the builder.
    pub fn push(&mut self, value: T) {
        self.buffer.push(value);
        self.valid.push(true);
    }

    /// Push an undefined value to the builder.
    pub fn push_undefined(&mut self) {
        self.buffer.push(T::default());
        self.valid.push(false);
    }

    /// Get the current length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Convert to CowVec and BitVec pair.
    pub fn into_parts(self) -> (CowVec<T>, BitVec) {
        (CowVec::new(self.buffer), self.valid)
    }
}

/// Enum to specify which pool type to use.
#[derive(Debug, Clone, Copy)]
pub enum PoolType {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    String,
    Bool,
}

/// Specialized builders for common numeric types.
impl PooledVectorBuilder<i32> {
    /// Create a new i32 builder using the pool.
    pub fn i32(capacity: usize, pool: Arc<BufferPoolManager>) -> Self {
        let buffer = pool.i32_pool.acquire(capacity).into_vec();
        let valid = pool.bool_pool.acquire_bitvec(capacity);

        Self {
            buffer,
            valid,
            pool_manager: pool,
        }
    }
}

impl PooledVectorBuilder<i64> {
    /// Create a new i64 builder using the pool.
    pub fn i64(capacity: usize, pool: Arc<BufferPoolManager>) -> Self {
        let buffer = pool.i64_pool.acquire(capacity).into_vec();
        let valid = pool.bool_pool.acquire_bitvec(capacity);

        Self {
            buffer,
            valid,
            pool_manager: pool,
        }
    }
}

impl PooledVectorBuilder<f32> {
    /// Create a new f32 builder using the pool.
    pub fn f32(capacity: usize, pool: Arc<BufferPoolManager>) -> Self {
        let buffer = pool.f32_pool.acquire(capacity).into_vec();
        let valid = pool.bool_pool.acquire_bitvec(capacity);

        Self {
            buffer,
            valid,
            pool_manager: pool,
        }
    }
}

impl PooledVectorBuilder<f64> {
    /// Create a new f64 builder using the pool.
    pub fn f64(capacity: usize, pool: Arc<BufferPoolManager>) -> Self {
        let buffer = pool.f64_pool.acquire(capacity).into_vec();
        let valid = pool.bool_pool.acquire_bitvec(capacity);

        Self {
            buffer,
            valid,
            pool_manager: pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::Type;

    #[test]
    fn test_pooled_column_builder() {
        let pool = BufferPoolManager::default();
        let builder = PooledColumnBuilder::with_capacity(&pool, Type::Int4, 100);
        let column = builder.build();

        assert_eq!(column.get_type(), Type::Int4);
        // Column should be empty initially but have capacity
        assert_eq!(column.len(), 0);
    }

    #[test]
    fn test_pooled_vector_builder_i32() {
        let pool = Arc::new(BufferPoolManager::default());
        let mut builder = PooledVectorBuilder::i32(100, pool);

        builder.push(42);
        builder.push(84);
        builder.push_undefined();

        assert_eq!(builder.len(), 3);

        let (values, valid) = builder.into_parts();
        assert_eq!(values.len(), 3);
        assert_eq!(valid.len(), 3);
        assert!(valid.get(0));
        assert!(valid.get(1));
        assert!(!valid.get(2));
    }

    #[test]
    fn test_column_values_ext() {
        let pool = BufferPoolManager::default();
        let column = ColumnValues::with_pooled_capacity(Type::Float8, 50, &pool);

        dbg!(&column);

        assert_eq!(column.get_type(), Type::Float8);
        assert_eq!(column.len(), 0);
    }

    #[test]
    fn test_pooled_vector_builder_types() {
        let pool = Arc::new(BufferPoolManager::default());

        let i32_builder = PooledVectorBuilder::i32(10, pool.clone());
        let i64_builder = PooledVectorBuilder::i64(10, pool.clone());
        let f32_builder = PooledVectorBuilder::f32(10, pool.clone());
        let f64_builder = PooledVectorBuilder::f64(10, pool.clone());

        assert_eq!(i32_builder.len(), 0);
        assert_eq!(i64_builder.len(), 0);
        assert_eq!(f32_builder.len(), 0);
        assert_eq!(f64_builder.len(), 0);
    }
}