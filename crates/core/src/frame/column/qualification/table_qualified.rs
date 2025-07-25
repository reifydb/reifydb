use crate::{BitVec};
use super::super::{ColumnValues, ColumnTableQualified, NewFrameColumn};

impl ColumnTableQualified {
    pub fn bool(table: &str, name: &str, values: impl IntoIterator<Item = bool>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::bool(values),
        })
    }

    pub fn bool_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::bool_with_bitvec(values, bitvec),
        })
    }

    pub fn float4(table: &str, name: &str, values: impl IntoIterator<Item = f32>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::float4(values),
        })
    }

    pub fn float4_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::float4_with_bitvec(values, bitvec),
        })
    }

    pub fn float8(table: &str, name: &str, values: impl IntoIterator<Item = f64>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::float8(values),
        })
    }

    pub fn float8_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::float8_with_bitvec(values, bitvec),
        })
    }

    pub fn int1(table: &str, name: &str, values: impl IntoIterator<Item = i8>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int1(values),
        })
    }

    pub fn int1_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int1_with_bitvec(values, bitvec),
        })
    }

    pub fn int2(table: &str, name: &str, values: impl IntoIterator<Item = i16>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int2(values),
        })
    }

    pub fn int2_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int2_with_bitvec(values, bitvec),
        })
    }

    pub fn int4(table: &str, name: &str, values: impl IntoIterator<Item = i32>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int4(values),
        })
    }

    pub fn int4_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int4_with_bitvec(values, bitvec),
        })
    }

    pub fn int8(table: &str, name: &str, values: impl IntoIterator<Item = i64>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int8(values),
        })
    }

    pub fn int8_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int8_with_bitvec(values, bitvec),
        })
    }

    pub fn int16(table: &str, name: &str, values: impl IntoIterator<Item = i128>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int16(values),
        })
    }

    pub fn int16_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::int16_with_bitvec(values, bitvec),
        })
    }

    pub fn uint1(table: &str, name: &str, values: impl IntoIterator<Item = u8>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint1(values),
        })
    }

    pub fn uint1_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint1_with_bitvec(values, bitvec),
        })
    }

    pub fn uint2(table: &str, name: &str, values: impl IntoIterator<Item = u16>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint2(values),
        })
    }

    pub fn uint2_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint2_with_bitvec(values, bitvec),
        })
    }

    pub fn uint4(table: &str, name: &str, values: impl IntoIterator<Item = u32>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint4(values),
        })
    }

    pub fn uint4_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint4_with_bitvec(values, bitvec),
        })
    }

    pub fn uint8(table: &str, name: &str, values: impl IntoIterator<Item = u64>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint8(values),
        })
    }

    pub fn uint8_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint8_with_bitvec(values, bitvec),
        })
    }

    pub fn uint16(table: &str, name: &str, values: impl IntoIterator<Item = u128>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint16(values),
        })
    }

    pub fn uint16_with_bitvec(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::uint16_with_bitvec(values, bitvec),
        })
    }

    pub fn utf8<'a>(table: &str, name: &str, values: impl IntoIterator<Item = &'a str>) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::utf8(values.into_iter().map(|s| s.to_string())),
        })
    }

    pub fn utf8_with_bitvec<'a>(
        table: &str,
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        bitvec: impl Into<BitVec>,
    ) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::utf8_with_bitvec(values.into_iter().map(|s| s.to_string()), bitvec),
        })
    }

    pub fn undefined(table: &str, name: &str, len: usize) -> NewFrameColumn {
        NewFrameColumn::TableQualified(Self {
            table: table.to_string(),
            name: name.to_string(),
            values: ColumnValues::undefined(len),
        })
    }
}