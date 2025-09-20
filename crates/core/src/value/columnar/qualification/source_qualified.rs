use reifydb_type::{Date, DateTime, Fragment, Interval, ROW_NUMBER_COLUMN_NAME, RowNumber, Time, Uuid4, Uuid7};

use super::super::{Column, ColumnData, SourceQualified};
use crate::BitVec;

impl<'a> SourceQualified<'a> {
	pub fn bool(source: &str, name: &str, data: impl IntoIterator<Item = bool>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::bool(data),
		})
	}

	pub fn bool_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		})
	}

	pub fn float4(source: &str, name: &str, data: impl IntoIterator<Item = f32>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::float4(data),
		})
	}

	pub fn float4_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::float4_with_bitvec(data, bitvec),
		})
	}

	pub fn float8(source: &str, name: &str, data: impl IntoIterator<Item = f64>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::float8(data),
		})
	}

	pub fn float8_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::float8_with_bitvec(data, bitvec),
		})
	}

	pub fn int1(source: &str, name: &str, data: impl IntoIterator<Item = i8>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int1(data),
		})
	}

	pub fn int1_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int1_with_bitvec(data, bitvec),
		})
	}

	pub fn int2(source: &str, name: &str, data: impl IntoIterator<Item = i16>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int2(data),
		})
	}

	pub fn int2_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int2_with_bitvec(data, bitvec),
		})
	}

	pub fn int4(source: &str, name: &str, data: impl IntoIterator<Item = i32>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int4(data),
		})
	}

	pub fn int4_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int4_with_bitvec(data, bitvec),
		})
	}

	pub fn int8(source: &str, name: &str, data: impl IntoIterator<Item = i64>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int8(data),
		})
	}

	pub fn int8_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int8_with_bitvec(data, bitvec),
		})
	}

	pub fn int16(source: &str, name: &str, data: impl IntoIterator<Item = i128>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int16(data),
		})
	}

	pub fn int16_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::int16_with_bitvec(data, bitvec),
		})
	}

	pub fn uint1(source: &str, name: &str, data: impl IntoIterator<Item = u8>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint1(data),
		})
	}

	pub fn uint1_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint1_with_bitvec(data, bitvec),
		})
	}

	pub fn uint2(source: &str, name: &str, data: impl IntoIterator<Item = u16>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint2(data),
		})
	}

	pub fn uint2_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint2_with_bitvec(data, bitvec),
		})
	}

	pub fn uint4(source: &str, name: &str, data: impl IntoIterator<Item = u32>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint4(data),
		})
	}

	pub fn uint4_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint4_with_bitvec(data, bitvec),
		})
	}

	pub fn uint8(source: &str, name: &str, data: impl IntoIterator<Item = u64>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint8(data),
		})
	}

	pub fn uint8_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint8_with_bitvec(data, bitvec),
		})
	}

	pub fn uint16(source: &str, name: &str, data: impl IntoIterator<Item = u128>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint16(data),
		})
	}

	pub fn uint16_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uint16_with_bitvec(data, bitvec),
		})
	}

	pub fn utf8<'b>(source: &str, name: &str, data: impl IntoIterator<Item = &'b str>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::utf8(data.into_iter().map(|s| s.to_string())),
		})
	}

	pub fn utf8_with_bitvec<'b>(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = &'b str>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::utf8_with_bitvec(data.into_iter().map(|s| s.to_string()), bitvec),
		})
	}

	pub fn undefined(source: &str, name: &str, len: usize) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::undefined(len),
		})
	}

	// Temporal types
	pub fn date(source: &str, name: &str, data: impl IntoIterator<Item = Date>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::date(data),
		})
	}

	pub fn date_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = Date>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::date_with_bitvec(data, bitvec),
		})
	}

	pub fn datetime(source: &str, name: &str, data: impl IntoIterator<Item = DateTime>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::datetime(data),
		})
	}

	pub fn datetime_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = DateTime>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::datetime_with_bitvec(data, bitvec),
		})
	}

	pub fn time(source: &str, name: &str, data: impl IntoIterator<Item = Time>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::time(data),
		})
	}

	pub fn time_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = Time>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::time_with_bitvec(data, bitvec),
		})
	}

	pub fn interval(source: &str, name: &str, data: impl IntoIterator<Item = Interval>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::interval(data),
		})
	}

	pub fn interval_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = Interval>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::interval_with_bitvec(data, bitvec),
		})
	}

	// UUID types
	pub fn uuid4(source: &str, name: &str, data: impl IntoIterator<Item = Uuid4>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uuid4(data),
		})
	}

	pub fn uuid4_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid4>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uuid4_with_bitvec(data, bitvec),
		})
	}

	pub fn uuid7(source: &str, name: &str, data: impl IntoIterator<Item = Uuid7>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uuid7(data),
		})
	}

	pub fn uuid7_with_bitvec(
		source: &str,
		name: &str,
		data: impl IntoIterator<Item = Uuid7>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(name),
			data: ColumnData::uuid7_with_bitvec(data, bitvec),
		})
	}

	pub fn row_number(source: &str, data: impl IntoIterator<Item = RowNumber>) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			data: ColumnData::row_number(data),
		})
	}

	pub fn row_number_with_bitvec(
		source: &str,
		data: impl IntoIterator<Item = RowNumber>,
		bitvec: impl Into<BitVec>,
	) -> Column<'a> {
		Column::SourceQualified(Self {
			source: Fragment::owned_internal(source),
			name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			data: ColumnData::row_number_with_bitvec(data, bitvec),
		})
	}
}
