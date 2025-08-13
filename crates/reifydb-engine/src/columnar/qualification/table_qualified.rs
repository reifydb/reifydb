use reifydb_core::{
	BitVec, Date, DateTime, Interval, RowId, Time, Uuid4, Uuid7,
	value::row_id::ROW_ID_COLUMN_NAME,
};

use super::super::{Column, ColumnData, TableQualified};

impl TableQualified {
	pub fn bool(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = bool>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::bool(data),
		})
	}

	pub fn bool_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		})
	}

	pub fn float4(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = f32>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::float4(data),
		})
	}

	pub fn float4_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::float4_with_bitvec(data, bitvec),
		})
	}

	pub fn float8(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = f64>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::float8(data),
		})
	}

	pub fn float8_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::float8_with_bitvec(data, bitvec),
		})
	}

	pub fn int1(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i8>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int1(data),
		})
	}

	pub fn int1_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int1_with_bitvec(data, bitvec),
		})
	}

	pub fn int2(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i16>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int2(data),
		})
	}

	pub fn int2_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int2_with_bitvec(data, bitvec),
		})
	}

	pub fn int4(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i32>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int4(data),
		})
	}

	pub fn int4_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int4_with_bitvec(data, bitvec),
		})
	}

	pub fn int8(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i64>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int8(data),
		})
	}

	pub fn int8_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int8_with_bitvec(data, bitvec),
		})
	}

	pub fn int16(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i128>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int16(data),
		})
	}

	pub fn int16_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::int16_with_bitvec(data, bitvec),
		})
	}

	pub fn uint1(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u8>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint1(data),
		})
	}

	pub fn uint1_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint1_with_bitvec(data, bitvec),
		})
	}

	pub fn uint2(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u16>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint2(data),
		})
	}

	pub fn uint2_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint2_with_bitvec(data, bitvec),
		})
	}

	pub fn uint4(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u32>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint4(data),
		})
	}

	pub fn uint4_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint4_with_bitvec(data, bitvec),
		})
	}

	pub fn uint8(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u64>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint8(data),
		})
	}

	pub fn uint8_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint8_with_bitvec(data, bitvec),
		})
	}

	pub fn uint16(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u128>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint16(data),
		})
	}

	pub fn uint16_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uint16_with_bitvec(data, bitvec),
		})
	}

	pub fn utf8<'a>(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = &'a str>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::utf8(
				data.into_iter().map(|s| s.to_string()),
			),
		})
	}

	pub fn utf8_with_bitvec<'a>(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = &'a str>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::utf8_with_bitvec(
				data.into_iter().map(|s| s.to_string()),
				bitvec,
			),
		})
	}

	pub fn undefined(table: &str, name: &str, len: usize) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::undefined(len),
		})
	}

	// Temporal types
	pub fn date(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Date>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::date(data),
		})
	}

	pub fn date_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Date>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::date_with_bitvec(data, bitvec),
		})
	}

	pub fn datetime(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = DateTime>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::datetime(data),
		})
	}

	pub fn datetime_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = DateTime>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::datetime_with_bitvec(data, bitvec),
		})
	}

	pub fn time(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Time>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::time(data),
		})
	}

	pub fn time_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Time>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::time_with_bitvec(data, bitvec),
		})
	}

	pub fn interval(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Interval>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::interval(data),
		})
	}

	pub fn interval_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Interval>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::interval_with_bitvec(data, bitvec),
		})
	}

	// UUID types
	pub fn uuid4(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid4>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uuid4(data),
		})
	}

	pub fn uuid4_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid4>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uuid4_with_bitvec(data, bitvec),
		})
	}

	pub fn uuid7(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid7>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uuid7(data),
		})
	}

	pub fn uuid7_with_bitvec(
		table: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid7>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: name.to_string(),
			data: ColumnData::uuid7_with_bitvec(data, bitvec),
		})
	}

	pub fn row_id(
		table: &str,
		data: impl IntoIterator<Item = RowId>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: ROW_ID_COLUMN_NAME.to_string(),
			data: ColumnData::row_id(data),
		})
	}

	pub fn row_id_with_bitvec(
		table: &str,
		data: impl IntoIterator<Item = RowId>,
		bitvec: impl Into<BitVec>,
	) -> Column {
		Column::TableQualified(Self {
			table: table.to_string(),
			name: ROW_ID_COLUMN_NAME.to_string(),
			data: ColumnData::row_id_with_bitvec(data, bitvec),
		})
	}
}
