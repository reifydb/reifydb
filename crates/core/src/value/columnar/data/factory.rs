// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	Blob, Date, DateTime, Decimal, IdentityId, Int, Interval, RowNumber,
	Time, Uint, Uuid4, Uuid7,
};

use crate::{
	BitVec,
	value::{
		columnar::ColumnData,
		container::{
			BlobContainer, BoolContainer, IdentityIdContainer,
			NumberContainer, RowNumberContainer, TemporalContainer,
			UndefinedContainer, Utf8Container, UuidContainer,
		},
	},
};

impl ColumnData {
	pub fn bool(data: impl IntoIterator<Item = bool>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Bool(BoolContainer::from_vec(data))
	}

	pub fn bool_with_capacity(capacity: usize) -> Self {
		ColumnData::Bool(BoolContainer::with_capacity(capacity))
	}

	pub fn bool_with_bitvec(
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Bool(BoolContainer::new(data, bitvec))
	}

	pub fn float4(data: impl IntoIterator<Item = f32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Float4(NumberContainer::from_vec(data))
	}

	pub fn float4_with_capacity(capacity: usize) -> Self {
		ColumnData::Float4(NumberContainer::with_capacity(capacity))
	}

	pub fn float4_with_bitvec(
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Float4(NumberContainer::new(data, bitvec))
	}

	pub fn float8(data: impl IntoIterator<Item = f64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Float8(NumberContainer::from_vec(data))
	}

	pub fn float8_with_capacity(capacity: usize) -> Self {
		ColumnData::Float8(NumberContainer::with_capacity(capacity))
	}

	pub fn float8_with_bitvec(
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Float8(NumberContainer::new(data, bitvec))
	}

	pub fn int1(data: impl IntoIterator<Item = i8>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int1(NumberContainer::from_vec(data))
	}

	pub fn int1_with_capacity(capacity: usize) -> Self {
		ColumnData::Int1(NumberContainer::with_capacity(capacity))
	}

	pub fn int1_with_bitvec(
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int1(NumberContainer::new(data, bitvec))
	}

	pub fn int2(data: impl IntoIterator<Item = i16>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int2(NumberContainer::from_vec(data))
	}

	pub fn int2_with_capacity(capacity: usize) -> Self {
		ColumnData::Int2(NumberContainer::with_capacity(capacity))
	}

	pub fn int2_with_bitvec(
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int2(NumberContainer::new(data, bitvec))
	}

	pub fn int4(data: impl IntoIterator<Item = i32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int4(NumberContainer::from_vec(data))
	}

	pub fn int4_with_capacity(capacity: usize) -> Self {
		ColumnData::Int4(NumberContainer::with_capacity(capacity))
	}

	pub fn int4_with_bitvec(
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int4(NumberContainer::new(data, bitvec))
	}

	pub fn int8(data: impl IntoIterator<Item = i64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int8(NumberContainer::from_vec(data))
	}

	pub fn int8_with_capacity(capacity: usize) -> Self {
		ColumnData::Int8(NumberContainer::with_capacity(capacity))
	}

	pub fn int8_with_bitvec(
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int8(NumberContainer::new(data, bitvec))
	}

	pub fn int16(data: impl IntoIterator<Item = i128>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int16(NumberContainer::from_vec(data))
	}

	pub fn int16_with_capacity(capacity: usize) -> Self {
		ColumnData::Int16(NumberContainer::with_capacity(capacity))
	}

	pub fn int16_with_bitvec(
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int16(NumberContainer::new(data, bitvec))
	}

	pub fn utf8(data: impl IntoIterator<Item = impl Into<String>>) -> Self {
		let data =
			data.into_iter().map(|c| c.into()).collect::<Vec<_>>();
		ColumnData::Utf8(Utf8Container::from_vec(data))
	}

	pub fn utf8_with_capacity(capacity: usize) -> Self {
		ColumnData::Utf8(Utf8Container::with_capacity(capacity))
	}

	pub fn utf8_with_bitvec<'a>(
		data: impl IntoIterator<Item = String>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Utf8(Utf8Container::new(data, bitvec))
	}

	pub fn uint1(data: impl IntoIterator<Item = u8>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint1(NumberContainer::from_vec(data))
	}

	pub fn uint1_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint1(NumberContainer::with_capacity(capacity))
	}

	pub fn uint1_with_bitvec(
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint1(NumberContainer::new(data, bitvec))
	}

	pub fn uint2(data: impl IntoIterator<Item = u16>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint2(NumberContainer::from_vec(data))
	}

	pub fn uint2_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint2(NumberContainer::with_capacity(capacity))
	}

	pub fn uint2_with_bitvec(
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint2(NumberContainer::new(data, bitvec))
	}

	pub fn uint4(data: impl IntoIterator<Item = u32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint4(NumberContainer::from_vec(data))
	}

	pub fn uint4_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint4(NumberContainer::with_capacity(capacity))
	}

	pub fn uint4_with_bitvec(
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint4(NumberContainer::new(data, bitvec))
	}

	pub fn uint8(data: impl IntoIterator<Item = u64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint8(NumberContainer::from_vec(data))
	}

	pub fn uint8_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint8(NumberContainer::with_capacity(capacity))
	}

	pub fn uint8_with_bitvec(
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint8(NumberContainer::new(data, bitvec))
	}

	pub fn uint16(data: impl IntoIterator<Item = u128>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint16(NumberContainer::from_vec(data))
	}

	pub fn uint16_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint16(NumberContainer::with_capacity(capacity))
	}

	pub fn uint16_with_bitvec(
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint16(NumberContainer::new(data, bitvec))
	}

	pub fn date(data: impl IntoIterator<Item = Date>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Date(TemporalContainer::from_vec(data))
	}

	pub fn date_with_capacity(capacity: usize) -> Self {
		ColumnData::Date(TemporalContainer::with_capacity(capacity))
	}

	pub fn date_with_bitvec(
		data: impl IntoIterator<Item = Date>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Date(TemporalContainer::new(data, bitvec))
	}

	pub fn datetime(data: impl IntoIterator<Item = DateTime>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::DateTime(TemporalContainer::from_vec(data))
	}

	pub fn datetime_with_capacity(capacity: usize) -> Self {
		ColumnData::DateTime(TemporalContainer::with_capacity(capacity))
	}

	pub fn datetime_with_bitvec(
		data: impl IntoIterator<Item = DateTime>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::DateTime(TemporalContainer::new(data, bitvec))
	}

	pub fn time(data: impl IntoIterator<Item = Time>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Time(TemporalContainer::from_vec(data))
	}

	pub fn time_with_capacity(capacity: usize) -> Self {
		ColumnData::Time(TemporalContainer::with_capacity(capacity))
	}

	pub fn time_with_bitvec(
		data: impl IntoIterator<Item = Time>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Time(TemporalContainer::new(data, bitvec))
	}

	pub fn interval(data: impl IntoIterator<Item = Interval>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Interval(TemporalContainer::from_vec(data))
	}

	pub fn interval_with_capacity(capacity: usize) -> Self {
		ColumnData::Interval(TemporalContainer::with_capacity(capacity))
	}

	pub fn interval_with_bitvec(
		data: impl IntoIterator<Item = Interval>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Interval(TemporalContainer::new(data, bitvec))
	}

	pub fn uuid4(data: impl IntoIterator<Item = Uuid4>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uuid4(UuidContainer::from_vec(data))
	}

	pub fn uuid4_with_capacity(capacity: usize) -> Self {
		ColumnData::Uuid4(UuidContainer::with_capacity(capacity))
	}

	pub fn uuid4_with_bitvec(
		data: impl IntoIterator<Item = Uuid4>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uuid4(UuidContainer::new(data, bitvec))
	}

	pub fn uuid7(data: impl IntoIterator<Item = Uuid7>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uuid7(UuidContainer::from_vec(data))
	}

	pub fn uuid7_with_capacity(capacity: usize) -> Self {
		ColumnData::Uuid7(UuidContainer::with_capacity(capacity))
	}

	pub fn uuid7_with_bitvec(
		data: impl IntoIterator<Item = Uuid7>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uuid7(UuidContainer::new(data, bitvec))
	}

	pub fn blob(data: impl IntoIterator<Item = Blob>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Blob(BlobContainer::from_vec(data))
	}

	pub fn blob_with_capacity(capacity: usize) -> Self {
		ColumnData::Blob(BlobContainer::with_capacity(capacity))
	}

	pub fn blob_with_bitvec(
		data: impl IntoIterator<Item = Blob>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Blob(BlobContainer::new(data, bitvec))
	}

	pub fn row_number(
		row_numbers: impl IntoIterator<Item = RowNumber>,
	) -> Self {
		let data = row_numbers.into_iter().collect::<Vec<_>>();
		ColumnData::RowNumber(RowNumberContainer::from_vec(data))
	}

	pub fn row_number_with_capacity(capacity: usize) -> Self {
		ColumnData::RowNumber(RowNumberContainer::with_capacity(
			capacity,
		))
	}

	pub fn row_number_with_bitvec(
		row_numbers: impl IntoIterator<Item = RowNumber>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = row_numbers.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::RowNumber(RowNumberContainer::new(data, bitvec))
	}

	pub fn identity_id(
		identity_ids: impl IntoIterator<Item = IdentityId>,
	) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		ColumnData::IdentityId(IdentityIdContainer::from_vec(data))
	}

	pub fn identity_id_with_capacity(capacity: usize) -> Self {
		ColumnData::IdentityId(IdentityIdContainer::with_capacity(
			capacity,
		))
	}

	pub fn identity_id_with_bitvec(
		identity_ids: impl IntoIterator<Item = IdentityId>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::IdentityId(IdentityIdContainer::new(data, bitvec))
	}

	pub fn int(data: impl IntoIterator<Item = Int>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int(NumberContainer::from_vec(data))
	}

	pub fn uint(data: impl IntoIterator<Item = Uint>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint(NumberContainer::from_vec(data))
	}

	pub fn int_with_capacity(capacity: usize) -> Self {
		ColumnData::Int(NumberContainer::with_capacity(capacity))
	}

	pub fn uint_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint(NumberContainer::with_capacity(capacity))
	}

	pub fn int_with_bitvec(
		data: impl IntoIterator<Item = Int>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int(NumberContainer::new(data, bitvec))
	}

	pub fn uint_with_bitvec(
		data: impl IntoIterator<Item = Uint>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint(NumberContainer::new(data, bitvec))
	}

	pub fn decimal(data: impl IntoIterator<Item = Decimal>) -> Self {
		use reifydb_type::value::constraint::{
			precision::Precision, scale::Scale,
		};
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Decimal {
			container: NumberContainer::from_vec(data),
			precision: Precision::new(38),
			scale: Scale::new(0),
		}
	}

	pub fn decimal_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::{
			precision::Precision, scale::Scale,
		};
		ColumnData::Decimal {
			container: NumberContainer::with_capacity(capacity),
			precision: Precision::new(38),
			scale: Scale::new(0),
		}
	}

	pub fn decimal_with_bitvec(
		data: impl IntoIterator<Item = Decimal>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		use reifydb_type::value::constraint::{
			precision::Precision, scale::Scale,
		};
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Decimal {
			container: NumberContainer::new(data, bitvec),
			precision: Precision::new(38),
			scale: Scale::new(0),
		}
	}

	pub fn undefined(len: usize) -> Self {
		ColumnData::Undefined(UndefinedContainer::new(len))
	}
}
