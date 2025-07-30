use super::super::{EngineColumnData, EngineColumn, FullyQualified};
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use reifydb_core::{BitVec, Date, DateTime, Interval, RowId, Time, Uuid4, Uuid7};

impl FullyQualified {
    pub fn bool(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = bool>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::bool(data),
        })
    }

    pub fn bool_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = bool>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::bool_with_bitvec(data, bitvec),
        })
    }

    pub fn float4(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = f32>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::float4(data),
        })
    }

    pub fn float4_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = f32>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::float4_with_bitvec(data, bitvec),
        })
    }

    pub fn float8(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = f64>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::float8(data),
        })
    }

    pub fn float8_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = f64>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::float8_with_bitvec(data, bitvec),
        })
    }

    pub fn int1(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i8>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int1(data),
        })
    }

    pub fn int1_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i8>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int1_with_bitvec(data, bitvec),
        })
    }

    pub fn int2(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i16>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int2(data),
        })
    }

    pub fn int2_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i16>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int2_with_bitvec(data, bitvec),
        })
    }

    pub fn int4(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i32>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int4(data),
        })
    }

    pub fn int4_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i32>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int4_with_bitvec(data, bitvec),
        })
    }

    pub fn int8(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i64>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int8(data),
        })
    }

    pub fn int8_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i64>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int8_with_bitvec(data, bitvec),
        })
    }

    pub fn int16(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i128>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int16(data),
        })
    }

    pub fn int16_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = i128>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::int16_with_bitvec(data, bitvec),
        })
    }

    pub fn uint1(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u8>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint1(data),
        })
    }

    pub fn uint1_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u8>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint1_with_bitvec(data, bitvec),
        })
    }

    pub fn uint2(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u16>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint2(data),
        })
    }

    pub fn uint2_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u16>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint2_with_bitvec(data, bitvec),
        })
    }

    pub fn uint4(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u32>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint4(data),
        })
    }

    pub fn uint4_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u32>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint4_with_bitvec(data, bitvec),
        })
    }

    pub fn uint8(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u64>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint8(data),
        })
    }

    pub fn uint8_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u64>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint8_with_bitvec(data, bitvec),
        })
    }

    pub fn uint16(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u128>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint16(data),
        })
    }

    pub fn uint16_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = u128>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uint16_with_bitvec(data, bitvec),
        })
    }

    pub fn utf8<'a>(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = &'a str>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::utf8(data.into_iter().map(|s| s.to_string())),
        })
    }

    pub fn utf8_with_bitvec<'a>(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = &'a str>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::utf8_with_bitvec(
                data.into_iter().map(|s| s.to_string()),
                bitvec,
            ),
        })
    }

    pub fn undefined(schema: &str, table: &str, name: &str, len: usize) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::undefined(len),
        })
    }

    // Temporal types
    pub fn date(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Date>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::date(data),
        })
    }

    pub fn date_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Date>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::date_with_bitvec(data, bitvec),
        })
    }

    pub fn datetime(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = DateTime>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::datetime(data),
        })
    }

    pub fn datetime_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = DateTime>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::datetime_with_bitvec(data, bitvec),
        })
    }

    pub fn time(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Time>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::time(data),
        })
    }

    pub fn time_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Time>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::time_with_bitvec(data, bitvec),
        })
    }

    pub fn interval(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Interval>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::interval(data),
        })
    }

    pub fn interval_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Interval>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::interval_with_bitvec(data, bitvec),
        })
    }

    // UUID types
    pub fn uuid4(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Uuid4>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uuid4(data),
        })
    }

    pub fn uuid4_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Uuid4>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uuid4_with_bitvec(data, bitvec),
        })
    }

    pub fn uuid7(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Uuid7>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uuid7(data),
        })
    }

    pub fn uuid7_with_bitvec(
        schema: &str,
        table: &str,
        name: &str,
        data: impl IntoIterator<Item = Uuid7>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            data: EngineColumnData::uuid7_with_bitvec(data, bitvec),
        })
    }

    pub fn row_id(
        schema: &str,
        table: &str,
        data: impl IntoIterator<Item = RowId>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: ROW_ID_COLUMN_NAME.to_string(),
            data: EngineColumnData::row_id(data),
        })
    }

    pub fn row_id_with_bitvec(
        schema: &str,
        table: &str,
        data: impl IntoIterator<Item = RowId>,
        bitvec: impl Into<BitVec>,
    ) -> EngineColumn {
        EngineColumn::FullyQualified(Self {
            schema: schema.to_string(),
            table: table.to_string(),
            name: ROW_ID_COLUMN_NAME.to_string(),
            data: EngineColumnData::row_id_with_bitvec(data, bitvec),
        })
    }
}
