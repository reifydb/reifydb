// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::row_id::ROW_ID_COLUMN_NAME;
use crate::{BitVec, Type};
pub use layout::FrameColumnLayout;
pub use push::Push;
pub use values::ColumnValues;

mod extend;
mod filter;
mod get;
mod layout;
mod push;
mod qualification;
mod reorder;
mod slice;
mod values;

#[derive(Clone, Debug, PartialEq)]
pub struct FrameColumn {
    // maybe name of the frame where this column was copied from, Some("users") or Some("orders") or None for expressions
    pub frame: Option<String>,
    // name of the column "id" or "user_id" or "1+2"
    pub name: String,
    pub values: ColumnValues,
}

impl FrameColumn {
    pub fn with_new_values(&self, values: ColumnValues) -> FrameColumn {
        Self { frame: self.frame.clone(), name: self.name.clone(), values }
    }

    pub fn fully_qualified(
        frame: impl Into<String>,
        name: impl Into<String>,
        values: ColumnValues,
    ) -> Self {
        let name = name.into();
        Self::validate_name(&name);
        Self { frame: Some(frame.into()), name, values }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnFullyQualified {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnTableQualified {
    pub table: String,
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnUnqualified {
    pub name: String,
    pub values: ColumnValues,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NewFrameColumn {
    FullyQualified(ColumnFullyQualified),
    TableQualified(ColumnTableQualified),
    Unqualified(ColumnUnqualified),
}

impl FrameColumn {
    pub fn get_type(&self) -> Type {
        self.values.get_type()
    }

    /// Returns the qualified name: frame.name if frame exists, otherwise just name
    pub fn qualified_name(&self) -> String {
        match &self.frame {
            Some(frame) => format!("{}.{}", frame, self.name),
            None => self.name.clone(),
        }
    }

    fn validate_name(name: &str) {
        if name == ROW_ID_COLUMN_NAME {
            panic!("Column name '{}' is reserved for RowId columns", ROW_ID_COLUMN_NAME);
        }
    }
}

impl FrameColumn {
    pub fn bool(frame: &str, name: &str, values: impl IntoIterator<Item = bool>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::bool(values))
    }

    pub fn bool_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::bool_with_bitvec(values, bitvec))
    }

    pub fn float4(frame: &str, name: &str, values: impl IntoIterator<Item = f32>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::float4(values))
    }

    pub fn float4_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::float4_with_bitvec(values, bitvec))
    }

    pub fn float8(frame: &str, name: &str, values: impl IntoIterator<Item = f64>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::float8(values))
    }

    pub fn float8_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::float8_with_bitvec(values, bitvec))
    }

    pub fn int1(frame: &str, name: &str, values: impl IntoIterator<Item = i8>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int1(values))
    }

    pub fn int1_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int1_with_bitvec(values, bitvec))
    }

    pub fn int2(frame: &str, name: &str, values: impl IntoIterator<Item = i16>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int2(values))
    }

    pub fn int2_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int2_with_bitvec(values, bitvec))
    }

    pub fn int4(frame: &str, name: &str, values: impl IntoIterator<Item = i32>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int4(values))
    }

    pub fn int4_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int4_with_bitvec(values, bitvec))
    }

    pub fn int8(frame: &str, name: &str, values: impl IntoIterator<Item = i64>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int8(values))
    }

    pub fn int8_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int8_with_bitvec(values, bitvec))
    }

    pub fn int16(frame: &str, name: &str, values: impl IntoIterator<Item = i128>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int16(values))
    }

    pub fn int16_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::int16_with_bitvec(values, bitvec))
    }

    pub fn uint1(frame: &str, name: &str, values: impl IntoIterator<Item = u8>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint1(values))
    }

    pub fn uint1_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint1_with_bitvec(values, bitvec))
    }

    pub fn uint2(frame: &str, name: &str, values: impl IntoIterator<Item = u16>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint2(values))
    }

    pub fn uint2_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint2_with_bitvec(values, bitvec))
    }

    pub fn uint4(frame: &str, name: &str, values: impl IntoIterator<Item = u32>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint4(values))
    }

    pub fn uint4_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint4_with_bitvec(values, bitvec))
    }

    pub fn uint8(frame: &str, name: &str, values: impl IntoIterator<Item = u64>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint8(values))
    }

    pub fn uint8_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint8_with_bitvec(values, bitvec))
    }

    pub fn uint16(frame: &str, name: &str, values: impl IntoIterator<Item = u128>) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint16(values))
    }

    pub fn uint16_with_bitvec(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::uint16_with_bitvec(values, bitvec))
    }

    pub fn utf8<'a>(frame: &str, name: &str, values: impl IntoIterator<Item = &'a str>) -> Self {
        Self::fully_qualified(
            frame,
            name,
            ColumnValues::utf8(values.into_iter().map(|s| s.to_string())),
        )
    }

    pub fn utf8_with_bitvec<'a>(
        frame: &str,
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        bitvec: impl Into<BitVec>,
    ) -> Self {
        Self::fully_qualified(
            frame,
            name,
            ColumnValues::utf8_with_bitvec(values.into_iter().map(|s| s.to_string()), bitvec),
        )
    }

    pub fn undefined(frame: &str, name: &str, len: usize) -> Self {
        Self::fully_qualified(frame, name, ColumnValues::undefined(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "Column name '__ROW__ID__' is reserved for RowId columns")]
    fn test_reserved_column_name_panic() {
        FrameColumn::int4("test_frame", ROW_ID_COLUMN_NAME, [1, 2, 3]);
    }

    #[test]
    fn test_normal_column_name_works() {
        let column = FrameColumn::int4("test_frame", "normal_column", [1, 2, 3]);
        assert_eq!(column.qualified_name(), "test_frame.normal_column");
        assert_eq!(column.frame, Some("test_frame".to_string()));
        assert_eq!(column.name, "normal_column");
    }
}
