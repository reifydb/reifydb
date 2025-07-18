// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use layout::FrameColumnLayout;
pub use push::Push;
use reifydb_core::{BitVec, Type};
pub use values::ColumnValues;

mod adjust;
mod extend;
mod filter;
mod get;
mod layout;
mod push;
mod reorder;
mod slice;
mod values;

#[derive(Clone, Debug, PartialEq)]
pub struct FrameColumn {
    pub name: String,
    pub values: ColumnValues,
}

impl FrameColumn {
    pub fn ty(&self) -> Type {
        self.values.ty()
    }
}

impl FrameColumn {
    pub fn bool(name: &str, values: impl IntoIterator<Item = bool>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::bool(values) }
    }

    pub fn bool_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::bool_with_bitvec(values, bitvec) }
    }

    pub fn float4(name: &str, values: impl IntoIterator<Item = f32>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::float4(values) }
    }

    pub fn float4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::float4_with_bitvec(values, bitvec) }
    }

    pub fn float8(name: &str, values: impl IntoIterator<Item = f64>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::float8(values) }
    }

    pub fn float8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::float8_with_bitvec(values, bitvec) }
    }

    pub fn int1(name: &str, values: impl IntoIterator<Item = i8>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int1(values) }
    }

    pub fn int1_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int1_with_bitvec(values, bitvec) }
    }

    pub fn int2(name: &str, values: impl IntoIterator<Item = i16>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int2(values) }
    }

    pub fn int2_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int2_with_bitvec(values, bitvec) }
    }

    pub fn int4(name: &str, values: impl IntoIterator<Item = i32>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int4(values) }
    }

    pub fn int4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int4_with_bitvec(values, bitvec) }
    }

    pub fn int8(name: &str, values: impl IntoIterator<Item = i64>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int8(values) }
    }

    pub fn int8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int8_with_bitvec(values, bitvec) }
    }

    pub fn int16(name: &str, values: impl IntoIterator<Item = i128>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int16(values) }
    }

    pub fn int16_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::int16_with_bitvec(values, bitvec) }
    }

    pub fn uint1(name: &str, values: impl IntoIterator<Item = u8>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint1(values) }
    }

    pub fn uint1_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint1_with_bitvec(values, bitvec) }
    }

    pub fn uint2(name: &str, values: impl IntoIterator<Item = u16>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint2(values) }
    }

    pub fn uint2_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint2_with_bitvec(values, bitvec) }
    }

    pub fn uint4(name: &str, values: impl IntoIterator<Item = u32>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint4(values) }
    }

    pub fn uint4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint4_with_bitvec(values, bitvec) }
    }

    pub fn uint8(name: &str, values: impl IntoIterator<Item = u64>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint8(values) }
    }

    pub fn uint8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint8_with_bitvec(values, bitvec) }
    }

    pub fn uint16(name: &str, values: impl IntoIterator<Item = u128>) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint16(values) }
    }

    pub fn uint16_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self { name: name.to_string(), values: ColumnValues::uint16_with_bitvec(values, bitvec) }
    }

    pub fn utf8<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            name: name.to_string(),
            values: ColumnValues::utf8(values.into_iter().map(|s| s.to_string())),
        }
    }

    pub fn utf8_with_bitvec<'a>(
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self {
            name: name.to_string(),
            values: ColumnValues::utf8_with_bitvec(
                values.into_iter().map(|s| s.to_string()),
                bitvec,
            ),
        }
    }

    pub fn undefined(name: &str, len: usize) -> Self {
        Self { name: name.to_string(), values: ColumnValues::undefined(len) }
    }
}
