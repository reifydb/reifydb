// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::frame::column::pool::{BufferPool, BufferedPools};
use crate::{CowVec, Type};

impl ColumnValues {
    pub fn new_pooled(target: Type, capacity: usize, pools: &BufferedPools) -> ColumnValues {
        match target {
            Type::Bool => {
                let values = pools.bool_pool.acquire_bitvec(capacity);
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Bool(values, bitvec)
            }
            Type::Int1 => {
                let buffer = pools.i8_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Int1(CowVec::new(vec), bitvec)
            }
            Type::Int2 => {
                let buffer = pools.i16_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Int2(CowVec::new(vec), bitvec)
            }
            Type::Int4 => {
                let buffer = pools.i32_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Int4(CowVec::new(vec), bitvec)
            }
            Type::Int8 => {
                let buffer = pools.i64_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Int8(CowVec::new(vec), bitvec)
            }
            Type::Int16 => {
                let buffer = pools.i128_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Int16(CowVec::new(vec), bitvec)
            }
            Type::Uint1 => {
                let buffer = pools.u8_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Uint1(CowVec::new(vec), bitvec)
            }
            Type::Uint2 => {
                let buffer = pools.u16_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Uint2(CowVec::new(vec), bitvec)
            }
            Type::Uint4 => {
                let buffer = pools.u32_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Uint4(CowVec::new(vec), bitvec)
            }
            Type::Uint8 => {
                let buffer = pools.u64_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Uint8(CowVec::new(vec), bitvec)
            }
            Type::Uint16 => {
                let buffer = pools.u128_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Uint16(CowVec::new(vec), bitvec)
            }
            Type::Float4 => {
                let buffer = pools.f32_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Float4(CowVec::new(vec), bitvec)
            }
            Type::Float8 => {
                let buffer = pools.f64_pool.acquire(capacity);
                let vec = buffer.into_vec();
                let bitvec = pools.bool_pool.acquire_bitvec(capacity);
                ColumnValues::Float8(CowVec::new(vec), bitvec)
            }
            Type::Utf8 => unimplemented!(),
            Type::Date => unimplemented!(),
            Type::DateTime => unimplemented!(),
            Type::Time => unimplemented!(),
            Type::Interval => unimplemented!(),
            Type::RowId => unimplemented!(),
            Type::Uuid4 => unimplemented!(),
            Type::Uuid7 => unimplemented!(),
            Type::Undefined => unimplemented!(),
            Type::Blob => unimplemented!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Type;

    #[test]
    fn test_pooled() {
        let pool = BufferedPools::default();
        let column = ColumnValues::new_pooled(Type::Int4, 100, &pool);

        assert_eq!(column.get_type(), Type::Int4);
        // Column should be empty initially but have capacity
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 100);
    }
}
