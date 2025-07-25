use super::super::{ColumnQualified, ColumnValues, FrameColumn};
use crate::BitVec;
use crate::value::row_id::ROW_ID_COLUMN_NAME;

impl ColumnQualified {
    pub fn bool(name: &str, values: impl IntoIterator<Item = bool>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::bool(values),
        })
    }

    pub fn bool_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::bool_with_bitvec(values, bitvec),
        })
    }

    pub fn float4(name: &str, values: impl IntoIterator<Item = f32>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::float4(values),
        })
    }

    pub fn float4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::float4_with_bitvec(values, bitvec),
        })
    }

    pub fn float8(name: &str, values: impl IntoIterator<Item = f64>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::float8(values),
        })
    }

    pub fn float8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::float8_with_bitvec(values, bitvec),
        })
    }

    pub fn int1(name: &str, values: impl IntoIterator<Item = i8>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int1(values),
        })
    }

    pub fn int1_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int1_with_bitvec(values, bitvec),
        })
    }

    pub fn int2(name: &str, values: impl IntoIterator<Item = i16>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int2(values),
        })
    }

    pub fn int2_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int2_with_bitvec(values, bitvec),
        })
    }

    pub fn int4(name: &str, values: impl IntoIterator<Item = i32>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int4(values),
        })
    }

    pub fn int4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int4_with_bitvec(values, bitvec),
        })
    }

    pub fn int8(name: &str, values: impl IntoIterator<Item = i64>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int8(values),
        })
    }

    pub fn int8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int8_with_bitvec(values, bitvec),
        })
    }

    pub fn int16(name: &str, values: impl IntoIterator<Item = i128>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int16(values),
        })
    }

    pub fn int16_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::int16_with_bitvec(values, bitvec),
        })
    }

    pub fn uint1(name: &str, values: impl IntoIterator<Item = u8>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint1(values),
        })
    }

    pub fn uint1_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint1_with_bitvec(values, bitvec),
        })
    }

    pub fn uint2(name: &str, values: impl IntoIterator<Item = u16>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint2(values),
        })
    }

    pub fn uint2_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint2_with_bitvec(values, bitvec),
        })
    }

    pub fn uint4(name: &str, values: impl IntoIterator<Item = u32>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint4(values),
        })
    }

    pub fn uint4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint4_with_bitvec(values, bitvec),
        })
    }

    pub fn uint8(name: &str, values: impl IntoIterator<Item = u64>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint8(values),
        })
    }

    pub fn uint8_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint8_with_bitvec(values, bitvec),
        })
    }

    pub fn uint16(name: &str, values: impl IntoIterator<Item = u128>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint16(values),
        })
    }

    pub fn uint16_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uint16_with_bitvec(values, bitvec),
        })
    }

    pub fn utf8<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::utf8(values.into_iter().map(|s| s.to_string())),
        })
    }

    pub fn utf8_with_bitvec<'a>(
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::utf8_with_bitvec(
                values.into_iter().map(|s| s.to_string()),
                bitvec,
            ),
        })
    }

    pub fn undefined(name: &str, len: usize) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::undefined(len),
        })
    }

    // Temporal types
    pub fn date(name: &str, values: impl IntoIterator<Item = crate::Date>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::date(values),
        })
    }

    pub fn date_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::Date>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::date_with_bitvec(values, bitvec),
        })
    }

    pub fn datetime(name: &str, values: impl IntoIterator<Item = crate::DateTime>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::datetime(values),
        })
    }

    pub fn datetime_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::DateTime>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::datetime_with_bitvec(values, bitvec),
        })
    }

    pub fn time(name: &str, values: impl IntoIterator<Item = crate::Time>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::time(values),
        })
    }

    pub fn time_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::Time>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::time_with_bitvec(values, bitvec),
        })
    }

    pub fn interval(name: &str, values: impl IntoIterator<Item = crate::Interval>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::interval(values),
        })
    }

    pub fn interval_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::Interval>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::interval_with_bitvec(values, bitvec),
        })
    }

    // UUID types
    pub fn uuid4(
        name: &str,
        values: impl IntoIterator<Item = crate::value::uuid::Uuid4>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uuid4(values),
        })
    }

    pub fn uuid4_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::value::uuid::Uuid4>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uuid4_with_bitvec(values, bitvec),
        })
    }

    pub fn uuid7(
        name: &str,
        values: impl IntoIterator<Item = crate::value::uuid::Uuid7>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uuid7(values),
        })
    }

    pub fn uuid7_with_bitvec(
        name: &str,
        values: impl IntoIterator<Item = crate::value::uuid::Uuid7>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: name.to_string(),
            values: ColumnValues::uuid7_with_bitvec(values, bitvec),
        })
    }

    pub fn row_id(values: impl IntoIterator<Item = crate::RowId>) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: ROW_ID_COLUMN_NAME.to_string(),
            values: ColumnValues::row_id(values),
        })
    }

    pub fn row_id_with_bitvec(
        values: impl IntoIterator<Item = crate::RowId>,
        bitvec: impl Into<BitVec>,
    ) -> FrameColumn {
        FrameColumn::ColumnQualified(Self {
            name: ROW_ID_COLUMN_NAME.to_string(),
            values: ColumnValues::row_id_with_bitvec(values, bitvec),
        })
    }
}
