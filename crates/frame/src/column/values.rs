// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueRef;
use reifydb_core::{CowVec, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValues {
    // value, is_valid
    Bool(CowVec<bool>, CowVec<bool>),
    Float4(CowVec<f32>, CowVec<bool>),
    Float8(CowVec<f64>, CowVec<bool>),
    Int1(CowVec<i8>, CowVec<bool>),
    Int2(CowVec<i16>, CowVec<bool>),
    Int4(CowVec<i32>, CowVec<bool>),
    Int8(CowVec<i64>, CowVec<bool>),
    Int16(CowVec<i128>, CowVec<bool>),
    String(CowVec<String>, CowVec<bool>),
    Uint1(CowVec<u8>, CowVec<bool>),
    Uint2(CowVec<u16>, CowVec<bool>),
    Uint4(CowVec<u32>, CowVec<bool>),
    Uint8(CowVec<u64>, CowVec<bool>),
    Uint16(CowVec<u128>, CowVec<bool>),
    // special case: all undefined
    Undefined(usize),
}

impl ColumnValues {
    pub fn bool(values: impl IntoIterator<Item = bool>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Bool(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn bool_with_validity(
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Bool(CowVec::new(values), CowVec::new(validity))
    }

    pub fn float4(values: impl IntoIterator<Item = f32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn float4_with_validity(
        values: impl IntoIterator<Item = f32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Float4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn float8(values: impl IntoIterator<Item = f64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Float8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn float8_with_validity(
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Float8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int1(values: impl IntoIterator<Item = i8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int1(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int1_with_validity(
        values: impl IntoIterator<Item = i8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int1(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int2(values: impl IntoIterator<Item = i16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int2(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int2_with_validity(
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int2(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int4(values: impl IntoIterator<Item = i32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int4_with_validity(
        values: impl IntoIterator<Item = i32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int8(values: impl IntoIterator<Item = i64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int8_with_validity(
        values: impl IntoIterator<Item = i64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn int16(values: impl IntoIterator<Item = i128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Int16(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn int16_with_validity(
        values: impl IntoIterator<Item = i128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Int16(CowVec::new(values), CowVec::new(validity))
    }

    pub fn string<'a>(values: impl IntoIterator<Item = String>) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::String(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn string_with_validity<'a>(
        values: impl IntoIterator<Item = String>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().map(|c| c.to_string()).collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::String(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint1(values: impl IntoIterator<Item = u8>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint1(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint1_with_validity(
        values: impl IntoIterator<Item = u8>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Uint1(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint2(values: impl IntoIterator<Item = u16>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint2(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint2_with_validity(
        values: impl IntoIterator<Item = u16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Uint2(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint4(values: impl IntoIterator<Item = u32>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint4(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint4_with_validity(
        values: impl IntoIterator<Item = u32>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Uint4(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint8(values: impl IntoIterator<Item = u64>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint8(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint8_with_validity(
        values: impl IntoIterator<Item = u64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Uint8(CowVec::new(values), CowVec::new(validity))
    }

    pub fn uint16(values: impl IntoIterator<Item = u128>) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let len = values.len();
        ColumnValues::Uint16(CowVec::new(values), CowVec::new(vec![true; len]))
    }

    pub fn uint16_with_validity(
        values: impl IntoIterator<Item = u128>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        let values = values.into_iter().collect::<Vec<_>>();
        let validity = validity.into_iter().collect::<Vec<_>>();
        debug_assert_eq!(validity.len(), values.len());
        ColumnValues::Uint16(CowVec::new(values), CowVec::new(validity))
    }

    pub fn undefined(len: usize) -> Self {
        ColumnValues::Undefined(len)
    }
}

impl ColumnValues {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            ColumnValues::Bool(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Float4(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Float8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int1(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int2(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int4(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int16(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::String(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint1(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint2(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint4(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint16(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Undefined(len) => {
                *len = indices.len();
            }
        }
    }
}

impl From<Value> for ColumnValues {
    fn from(value: Value) -> Self {
        match value {
            Value::Bool(v) => ColumnValues::bool([v]),
            Value::Float4(v) => ColumnValues::float4([v.value()]),
            Value::Float8(v) => ColumnValues::float8([v.value()]),
            Value::Int1(v) => ColumnValues::int1([v]),
            Value::Int2(v) => ColumnValues::int2([v]),
            Value::Int4(v) => ColumnValues::int4([v]),
            Value::Int8(v) => ColumnValues::int8([v]),
            Value::Int16(v) => ColumnValues::int16([v]),
            Value::String(v) => ColumnValues::string([v]),
            Value::Uint1(v) => ColumnValues::uint1([v]),
            Value::Uint2(v) => ColumnValues::uint2([v]),
            Value::Uint4(v) => ColumnValues::uint4([v]),
            Value::Uint8(v) => ColumnValues::uint8([v]),
            Value::Uint16(v) => ColumnValues::uint16([v]),
            Value::Undefined => ColumnValues::undefined(1),
        }
    }
}

impl ColumnValues {
    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Bool(_, b) => b.len(),
            ColumnValues::Float4(_, b) => b.len(),
            ColumnValues::Float8(_, b) => b.len(),
            ColumnValues::Int1(_, b) => b.len(),
            ColumnValues::Int2(_, b) => b.len(),
            ColumnValues::Int4(_, b) => b.len(),
            ColumnValues::Int8(_, b) => b.len(),
            ColumnValues::Int16(_, b) => b.len(),
            ColumnValues::String(_, b) => b.len(),
            ColumnValues::Uint1(_, b) => b.len(),
            ColumnValues::Uint2(_, b) => b.len(),
            ColumnValues::Uint4(_, b) => b.len(),
            ColumnValues::Uint8(_, b) => b.len(),
            ColumnValues::Uint16(_, b) => b.len(),
            ColumnValues::Undefined(n) => *n,
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
