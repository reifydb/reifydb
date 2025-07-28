// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{BufferPool, BufferPoolManager};
use reifydb_core::frame::ColumnValues;
use reifydb_core::{CowVec, Type};

/// Builder for ColumnValues that uses buffer pools for efficient memory allocation.
pub struct PooledColumnBuilder<'a> {
    pool: &'a BufferPoolManager,
    target: Type,
    capacity: usize,
}

impl<'a> PooledColumnBuilder<'a> {
    /// Create a new pooled column builder with the specified type and capacity.
    pub fn with_capacity(pool: &'a BufferPoolManager, target_type: Type, capacity: usize) -> Self {
        Self { pool, target: target_type, capacity }
    }

    /// Build the ColumnValues using pooled buffers where possible.
    pub fn build(self) -> ColumnValues {
        match self.target {
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

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::Type;

    #[test]
    fn test_pooled_column_builder() {
        let mut pool = BufferPoolManager::default();
        let builder = PooledColumnBuilder::with_capacity(&mut pool, Type::Int4, 100);
        let column = builder.build();

        assert_eq!(column.get_type(), Type::Int4);
        // Column should be empty initially but have capacity
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 100);
    }
}
