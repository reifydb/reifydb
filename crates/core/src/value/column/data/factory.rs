// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, undefined::UndefinedContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::ColumnData;

impl ColumnData {
	pub fn bool(data: impl IntoIterator<Item = bool>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Bool(BoolContainer::from_vec(data))
	}

	pub fn bool_optional(data: impl IntoIterator<Item = Option<bool>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(false);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Bool(BoolContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn bool_with_capacity(capacity: usize) -> Self {
		ColumnData::Bool(BoolContainer::with_capacity(capacity))
	}

	pub fn bool_with_bitvec(data: impl IntoIterator<Item = bool>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Bool(BoolContainer::new(data, bitvec))
	}

	pub fn float4(data: impl IntoIterator<Item = f32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Float4(NumberContainer::from_vec(data))
	}

	pub fn float4_optional(data: impl IntoIterator<Item = Option<f32>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0.0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Float4(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn float4_with_capacity(capacity: usize) -> Self {
		ColumnData::Float4(NumberContainer::with_capacity(capacity))
	}

	pub fn float4_with_bitvec(data: impl IntoIterator<Item = f32>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Float4(NumberContainer::new(data, bitvec))
	}

	pub fn float8(data: impl IntoIterator<Item = f64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Float8(NumberContainer::from_vec(data))
	}

	pub fn float8_optional(data: impl IntoIterator<Item = Option<f64>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0.0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Float8(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn float8_with_capacity(capacity: usize) -> Self {
		ColumnData::Float8(NumberContainer::with_capacity(capacity))
	}

	pub fn float8_with_bitvec(data: impl IntoIterator<Item = f64>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Float8(NumberContainer::new(data, bitvec))
	}

	pub fn int1(data: impl IntoIterator<Item = i8>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int1(NumberContainer::from_vec(data))
	}

	pub fn int1_optional(data: impl IntoIterator<Item = Option<i8>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int1(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn int1_with_capacity(capacity: usize) -> Self {
		ColumnData::Int1(NumberContainer::with_capacity(capacity))
	}

	pub fn int1_with_bitvec(data: impl IntoIterator<Item = i8>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int1(NumberContainer::new(data, bitvec))
	}

	pub fn int2(data: impl IntoIterator<Item = i16>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int2(NumberContainer::from_vec(data))
	}

	pub fn int2_optional(data: impl IntoIterator<Item = Option<i16>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int2(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn int2_with_capacity(capacity: usize) -> Self {
		ColumnData::Int2(NumberContainer::with_capacity(capacity))
	}

	pub fn int2_with_bitvec(data: impl IntoIterator<Item = i16>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int2(NumberContainer::new(data, bitvec))
	}

	pub fn int4(data: impl IntoIterator<Item = i32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int4(NumberContainer::from_vec(data))
	}

	pub fn int4_optional(data: impl IntoIterator<Item = Option<i32>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int4(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn int4_with_capacity(capacity: usize) -> Self {
		ColumnData::Int4(NumberContainer::with_capacity(capacity))
	}

	pub fn int4_with_bitvec(data: impl IntoIterator<Item = i32>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int4(NumberContainer::new(data, bitvec))
	}

	pub fn int8(data: impl IntoIterator<Item = i64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int8(NumberContainer::from_vec(data))
	}

	pub fn int8_optional(data: impl IntoIterator<Item = Option<i64>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int8(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn int8_with_capacity(capacity: usize) -> Self {
		ColumnData::Int8(NumberContainer::with_capacity(capacity))
	}

	pub fn int8_with_bitvec(data: impl IntoIterator<Item = i64>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int8(NumberContainer::new(data, bitvec))
	}

	pub fn int16(data: impl IntoIterator<Item = i128>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int16(NumberContainer::from_vec(data))
	}

	pub fn int16_optional(data: impl IntoIterator<Item = Option<i128>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int16(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn int16_with_capacity(capacity: usize) -> Self {
		ColumnData::Int16(NumberContainer::with_capacity(capacity))
	}

	pub fn int16_with_bitvec(data: impl IntoIterator<Item = i128>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int16(NumberContainer::new(data, bitvec))
	}

	pub fn utf8(data: impl IntoIterator<Item = impl Into<String>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().map(|c| c.into()).collect::<Vec<_>>();
		ColumnData::Utf8 {
			container: Utf8Container::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_optional(data: impl IntoIterator<Item = Option<String>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(String::new());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Utf8 {
			container: Utf8Container::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Utf8 {
			container: Utf8Container::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_with_bitvec(data: impl IntoIterator<Item = impl Into<String>>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().map(Into::into).collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Utf8 {
			container: Utf8Container::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint1(data: impl IntoIterator<Item = u8>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint1(NumberContainer::from_vec(data))
	}

	pub fn uint1_optional(data: impl IntoIterator<Item = Option<u8>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint1(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uint1_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint1(NumberContainer::with_capacity(capacity))
	}

	pub fn uint1_with_bitvec(data: impl IntoIterator<Item = u8>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint1(NumberContainer::new(data, bitvec))
	}

	pub fn uint2(data: impl IntoIterator<Item = u16>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint2(NumberContainer::from_vec(data))
	}

	pub fn uint2_optional(data: impl IntoIterator<Item = Option<u16>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint2(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uint2_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint2(NumberContainer::with_capacity(capacity))
	}

	pub fn uint2_with_bitvec(data: impl IntoIterator<Item = u16>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint2(NumberContainer::new(data, bitvec))
	}

	pub fn uint4(data: impl IntoIterator<Item = u32>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint4(NumberContainer::from_vec(data))
	}

	pub fn uint4_optional(data: impl IntoIterator<Item = Option<u32>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint4(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uint4_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint4(NumberContainer::with_capacity(capacity))
	}

	pub fn uint4_with_bitvec(data: impl IntoIterator<Item = u32>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint4(NumberContainer::new(data, bitvec))
	}

	pub fn uint8(data: impl IntoIterator<Item = u64>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint8(NumberContainer::from_vec(data))
	}

	pub fn uint8_optional(data: impl IntoIterator<Item = Option<u64>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint8(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uint8_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint8(NumberContainer::with_capacity(capacity))
	}

	pub fn uint8_with_bitvec(data: impl IntoIterator<Item = u64>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint8(NumberContainer::new(data, bitvec))
	}

	pub fn uint16(data: impl IntoIterator<Item = u128>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint16(NumberContainer::from_vec(data))
	}

	pub fn uint16_optional(data: impl IntoIterator<Item = Option<u128>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint16(NumberContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uint16_with_capacity(capacity: usize) -> Self {
		ColumnData::Uint16(NumberContainer::with_capacity(capacity))
	}

	pub fn uint16_with_bitvec(data: impl IntoIterator<Item = u128>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint16(NumberContainer::new(data, bitvec))
	}

	pub fn date(data: impl IntoIterator<Item = Date>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Date(TemporalContainer::from_vec(data))
	}

	pub fn date_optional(data: impl IntoIterator<Item = Option<Date>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Date::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Date(TemporalContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn date_with_capacity(capacity: usize) -> Self {
		ColumnData::Date(TemporalContainer::with_capacity(capacity))
	}

	pub fn date_with_bitvec(data: impl IntoIterator<Item = Date>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Date(TemporalContainer::new(data, bitvec))
	}

	pub fn datetime(data: impl IntoIterator<Item = DateTime>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::DateTime(TemporalContainer::from_vec(data))
	}

	pub fn datetime_optional(data: impl IntoIterator<Item = Option<DateTime>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(DateTime::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::DateTime(TemporalContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn datetime_with_capacity(capacity: usize) -> Self {
		ColumnData::DateTime(TemporalContainer::with_capacity(capacity))
	}

	pub fn datetime_with_bitvec(data: impl IntoIterator<Item = DateTime>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::DateTime(TemporalContainer::new(data, bitvec))
	}

	pub fn time(data: impl IntoIterator<Item = Time>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Time(TemporalContainer::from_vec(data))
	}

	pub fn time_optional(data: impl IntoIterator<Item = Option<Time>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Time::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Time(TemporalContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn time_with_capacity(capacity: usize) -> Self {
		ColumnData::Time(TemporalContainer::with_capacity(capacity))
	}

	pub fn time_with_bitvec(data: impl IntoIterator<Item = Time>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Time(TemporalContainer::new(data, bitvec))
	}

	pub fn duration(data: impl IntoIterator<Item = Duration>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Duration(TemporalContainer::from_vec(data))
	}

	pub fn duration_optional(data: impl IntoIterator<Item = Option<Duration>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Duration::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Duration(TemporalContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn duration_with_capacity(capacity: usize) -> Self {
		ColumnData::Duration(TemporalContainer::with_capacity(capacity))
	}

	pub fn duration_with_bitvec(data: impl IntoIterator<Item = Duration>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Duration(TemporalContainer::new(data, bitvec))
	}

	pub fn uuid4(data: impl IntoIterator<Item = Uuid4>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uuid4(UuidContainer::from_vec(data))
	}

	pub fn uuid4_optional(data: impl IntoIterator<Item = Option<Uuid4>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Uuid4::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uuid4(UuidContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uuid4_with_capacity(capacity: usize) -> Self {
		ColumnData::Uuid4(UuidContainer::with_capacity(capacity))
	}

	pub fn uuid4_with_bitvec(data: impl IntoIterator<Item = Uuid4>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uuid4(UuidContainer::new(data, bitvec))
	}

	pub fn uuid7(data: impl IntoIterator<Item = Uuid7>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uuid7(UuidContainer::from_vec(data))
	}

	pub fn uuid7_optional(data: impl IntoIterator<Item = Option<Uuid7>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Uuid7::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uuid7(UuidContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn uuid7_with_capacity(capacity: usize) -> Self {
		ColumnData::Uuid7(UuidContainer::with_capacity(capacity))
	}

	pub fn uuid7_with_bitvec(data: impl IntoIterator<Item = Uuid7>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uuid7(UuidContainer::new(data, bitvec))
	}

	pub fn blob(data: impl IntoIterator<Item = Blob>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Blob {
			container: BlobContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_optional(data: impl IntoIterator<Item = Option<Blob>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Blob::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Blob {
			container: BlobContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Blob {
			container: BlobContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_with_bitvec(data: impl IntoIterator<Item = Blob>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Blob {
			container: BlobContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn identity_id(identity_ids: impl IntoIterator<Item = IdentityId>) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		ColumnData::IdentityId(IdentityIdContainer::from_vec(data))
	}

	pub fn identity_id_optional(identity_ids: impl IntoIterator<Item = Option<IdentityId>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in identity_ids {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(IdentityId::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::IdentityId(IdentityIdContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn identity_id_with_capacity(capacity: usize) -> Self {
		ColumnData::IdentityId(IdentityIdContainer::with_capacity(capacity))
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
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn int_optional(data: impl IntoIterator<Item = Option<Int>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Int::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint(data: impl IntoIterator<Item = Uint>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_optional(data: impl IntoIterator<Item = Option<Uint>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Uint::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn int_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Int {
			container: NumberContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Uint {
			container: NumberContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn int_with_bitvec(data: impl IntoIterator<Item = Int>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Int {
			container: NumberContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_with_bitvec(data: impl IntoIterator<Item = Uint>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint {
			container: NumberContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn decimal(data: impl IntoIterator<Item = Decimal>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Decimal {
			container: NumberContainer::from_vec(data),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn decimal_optional(data: impl IntoIterator<Item = Option<Decimal>>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Decimal::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Decimal {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn decimal_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		ColumnData::Decimal {
			container: NumberContainer::with_capacity(capacity),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn decimal_with_bitvec(data: impl IntoIterator<Item = Decimal>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Decimal {
			container: NumberContainer::new(data, bitvec),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn any(data: impl IntoIterator<Item = Box<reifydb_type::value::Value>>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Any(AnyContainer::from_vec(data))
	}

	pub fn any_optional(data: impl IntoIterator<Item = Option<Box<reifydb_type::value::Value>>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Box::new(reifydb_type::value::Value::Undefined));
					bitvec.push(false);
				}
			}
		}

		ColumnData::Any(AnyContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn any_with_capacity(capacity: usize) -> Self {
		ColumnData::Any(AnyContainer::with_capacity(capacity))
	}

	pub fn any_with_bitvec(
		data: impl IntoIterator<Item = Box<reifydb_type::value::Value>>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Any(AnyContainer::new(data, bitvec))
	}

	pub fn undefined(len: usize) -> Self {
		ColumnData::Undefined(UndefinedContainer::new(len))
	}

	/// Create typed column data with all undefined values (bitvec all false).
	pub fn undefined_typed(ty: Type, len: usize) -> Self {
		match ty {
			Type::Boolean => Self::bool_with_bitvec(vec![false; len], BitVec::repeat(len, false)),
			Type::Float4 => Self::float4_with_bitvec(vec![0.0f32; len], BitVec::repeat(len, false)),
			Type::Float8 => Self::float8_with_bitvec(vec![0.0f64; len], BitVec::repeat(len, false)),
			Type::Int1 => Self::int1_with_bitvec(vec![0i8; len], BitVec::repeat(len, false)),
			Type::Int2 => Self::int2_with_bitvec(vec![0i16; len], BitVec::repeat(len, false)),
			Type::Int4 => Self::int4_with_bitvec(vec![0i32; len], BitVec::repeat(len, false)),
			Type::Int8 => Self::int8_with_bitvec(vec![0i64; len], BitVec::repeat(len, false)),
			Type::Int16 => Self::int16_with_bitvec(vec![0i128; len], BitVec::repeat(len, false)),
			Type::Utf8 => Self::utf8_with_bitvec(vec![String::new(); len], BitVec::repeat(len, false)),
			Type::Uint1 => Self::uint1_with_bitvec(vec![0u8; len], BitVec::repeat(len, false)),
			Type::Uint2 => Self::uint2_with_bitvec(vec![0u16; len], BitVec::repeat(len, false)),
			Type::Uint4 => Self::uint4_with_bitvec(vec![0u32; len], BitVec::repeat(len, false)),
			Type::Uint8 => Self::uint8_with_bitvec(vec![0u64; len], BitVec::repeat(len, false)),
			Type::Uint16 => Self::uint16_with_bitvec(vec![0u128; len], BitVec::repeat(len, false)),
			Type::Date => Self::date_with_bitvec(vec![Date::default(); len], BitVec::repeat(len, false)),
			Type::DateTime => {
				Self::datetime_with_bitvec(vec![DateTime::default(); len], BitVec::repeat(len, false))
			}
			Type::Time => Self::time_with_bitvec(vec![Time::default(); len], BitVec::repeat(len, false)),
			Type::Duration => {
				Self::duration_with_bitvec(vec![Duration::default(); len], BitVec::repeat(len, false))
			}
			Type::Blob => Self::blob_with_bitvec(vec![Blob::new(vec![]); len], BitVec::repeat(len, false)),
			Type::Uuid4 => Self::uuid4_with_bitvec(vec![Uuid4::default(); len], BitVec::repeat(len, false)),
			Type::Uuid7 => Self::uuid7_with_bitvec(vec![Uuid7::default(); len], BitVec::repeat(len, false)),
			Type::IdentityId => Self::identity_id_with_bitvec(
				vec![IdentityId::default(); len],
				BitVec::repeat(len, false),
			),
			Type::Int => Self::int_with_bitvec(vec![Int::default(); len], BitVec::repeat(len, false)),
			Type::Uint => Self::uint_with_bitvec(vec![Uint::default(); len], BitVec::repeat(len, false)),
			Type::Decimal {
				..
			} => Self::decimal_with_bitvec(vec![Decimal::from(0); len], BitVec::repeat(len, false)),
			Type::Any => Self::any_with_bitvec(
				vec![Box::new(reifydb_type::value::Value::Undefined); len],
				BitVec::repeat(len, false),
			),
			Type::Undefined => Self::undefined(len),
		}
	}
}
