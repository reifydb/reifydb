// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	BooleanArray, LargeStringArray, PrimitiveArray,
	builder::{LargeBinaryBuilder, LargeStringBuilder},
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer, NullBuffer, ScalarBuffer};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
	container::{
		any_array::any_array,
		decimal_array::{
			DecimalArray, decimal_array, int16_array, uint16_array, with_int16_type, with_uint16_type,
		},
		dictionary_array::{self, DICTIONARY_ENTRY_WIDTH, dictionary_array},
		digest_array::digest_array,
		temporal_array::{DATETIME_TIMEZONE, date_array, datetime_array, duration_array, time_array},
		uuid_array::{self, UUID_WIDTH, identity_id_array, uuid4_array, uuid7_array},
		varlen_array::blob_array,
	},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

use crate::value::column::ColumnBuffer;

macro_rules! impl_native_factory {
	($name:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnBuffer::$variant(PrimitiveArray::new(ScalarBuffer::from(data), None))
		}

		pub(crate) fn $name_cap(capacity: usize) -> Self {
			ColumnBuffer::$variant(PrimitiveArray::new(
				ScalarBuffer::from(Vec::with_capacity(capacity)),
				None,
			))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BooleanBuffer>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnBuffer::$variant(PrimitiveArray::new(ScalarBuffer::from(data), None));
			if !bitvec.has_false() {
				inner
			} else {
				inner.with_nulls(NullBuffer::new(bitvec))
			}
		}
	};
}

macro_rules! impl_number_factory {
	($name:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty, $build:ident, $with_type:ident) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			ColumnBuffer::$variant($build(data))
		}

		pub(crate) fn $name_cap(capacity: usize) -> Self {
			ColumnBuffer::$variant($with_type(PrimitiveArray::new(
				ScalarBuffer::from(Vec::with_capacity(capacity)),
				None,
			)))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BooleanBuffer>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnBuffer::$variant($build(data));
			if !bitvec.has_false() {
				inner
			} else {
				inner.with_nulls(NullBuffer::new(bitvec))
			}
		}
	};
}

macro_rules! impl_temporal_factory {
	($name:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty, $build:ident) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnBuffer::$variant($build(data))
		}

		pub(crate) fn $name_cap(capacity: usize) -> Self {
			ColumnBuffer::$variant(PrimitiveArray::new(
				ScalarBuffer::from(Vec::with_capacity(capacity)),
				None,
			))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BooleanBuffer>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnBuffer::$variant($build(data));
			if !bitvec.has_false() {
				inner
			} else {
				inner.with_nulls(NullBuffer::new(bitvec))
			}
		}
	};
}

macro_rules! impl_uuid_factory {
	($name:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty, $build:ident) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnBuffer::$variant($build(data))
		}

		pub(crate) fn $name_cap(capacity: usize) -> Self {
			ColumnBuffer::$variant(uuid_array::from_buffer(MutableBuffer::with_capacity(
				capacity * UUID_WIDTH,
			)))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BooleanBuffer>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnBuffer::$variant($build(data));
			if !bitvec.has_false() {
				inner
			} else {
				inner.with_nulls(NullBuffer::new(bitvec))
			}
		}
	};
}

impl ColumnBuffer {
	pub fn bool(data: impl IntoIterator<Item = bool>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnBuffer::Bool(BooleanArray::from(data))
	}

	pub(crate) fn bool_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::Bool(BooleanArray::from(BooleanBufferBuilder::new(capacity).finish()))
	}

	pub fn bool_with_bitvec(data: impl IntoIterator<Item = bool>, bitvec: impl Into<BooleanBuffer>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Bool(BooleanArray::from(data));
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	impl_native_factory!(float4, float4_with_capacity, float4_with_bitvec, Float4, f32);
	impl_native_factory!(float8, float8_with_capacity, float8_with_bitvec, Float8, f64);
	impl_native_factory!(int1, int1_with_capacity, int1_with_bitvec, Int1, i8);
	impl_native_factory!(int2, int2_with_capacity, int2_with_bitvec, Int2, i16);
	impl_native_factory!(int4, int4_with_capacity, int4_with_bitvec, Int4, i32);

	pub fn int4_optional(data: impl IntoIterator<Item = Option<i32>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;
		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(0);
					bitvec.push(false);
					has_none = true;
				}
			}
		}
		let inner = ColumnBuffer::Int4(PrimitiveArray::new(ScalarBuffer::from(values), None));
		if has_none {
			inner.with_nulls(NullBuffer::new(BooleanBuffer::from(bitvec)))
		} else {
			inner
		}
	}

	impl_native_factory!(int8, int8_with_capacity, int8_with_bitvec, Int8, i64);
	impl_number_factory!(int16, int16_with_capacity, int16_with_bitvec, Int16, i128, int16_array, with_int16_type);
	impl_native_factory!(uint1, uint1_with_capacity, uint1_with_bitvec, Uint1, u8);
	impl_native_factory!(uint2, uint2_with_capacity, uint2_with_bitvec, Uint2, u16);
	impl_native_factory!(uint4, uint4_with_capacity, uint4_with_bitvec, Uint4, u32);
	impl_native_factory!(uint8, uint8_with_capacity, uint8_with_bitvec, Uint8, u64);
	impl_number_factory!(
		uint16,
		uint16_with_capacity,
		uint16_with_bitvec,
		Uint16,
		u128,
		uint16_array,
		with_uint16_type
	);

	pub fn utf8(data: impl IntoIterator<Item = impl Into<String>>) -> Self {
		let data = data.into_iter().map(|c| c.into()).collect::<Vec<String>>();
		ColumnBuffer::Utf8 {
			container: LargeStringArray::from(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_repeated(value: &str, count: usize) -> Self {
		ColumnBuffer::Utf8 {
			container: LargeStringArray::new_repeated(value, count),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub(crate) fn utf8_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::Utf8 {
			container: LargeStringBuilder::with_capacity(capacity, capacity * 16).finish(),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_with_bitvec(
		data: impl IntoIterator<Item = impl Into<String>>,
		bitvec: impl Into<BooleanBuffer>,
	) -> Self {
		let data = data.into_iter().map(Into::into).collect::<Vec<String>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Utf8 {
			container: LargeStringArray::from(data),
			max_bytes: MaxBytes::MAX,
		};
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	impl_temporal_factory!(date, date_with_capacity, date_with_bitvec, Date, Date, date_array);

	pub fn datetime(data: impl IntoIterator<Item = DateTime>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnBuffer::DateTime(datetime_array(data))
	}

	pub(crate) fn datetime_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::DateTime(
			PrimitiveArray::new(ScalarBuffer::from(Vec::with_capacity(capacity)), None)
				.with_timezone(DATETIME_TIMEZONE),
		)
	}

	pub fn datetime_with_bitvec(
		data: impl IntoIterator<Item = DateTime>,
		bitvec: impl Into<BooleanBuffer>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::DateTime(datetime_array(data));
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	impl_temporal_factory!(time, time_with_capacity, time_with_bitvec, Time, Time, time_array);
	impl_temporal_factory!(
		duration,
		duration_with_capacity,
		duration_with_bitvec,
		Duration,
		Duration,
		duration_array
	);

	impl_uuid_factory!(uuid4, uuid4_with_capacity, uuid4_with_bitvec, Uuid4, Uuid4, uuid4_array);
	impl_uuid_factory!(uuid7, uuid7_with_capacity, uuid7_with_bitvec, Uuid7, Uuid7, uuid7_array);

	pub fn blob(data: impl IntoIterator<Item = Blob>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnBuffer::Blob {
			container: blob_array(&data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub(crate) fn blob_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::Blob {
			container: LargeBinaryBuilder::with_capacity(capacity, capacity * 32).finish(),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_with_bitvec(data: impl IntoIterator<Item = Blob>, bitvec: impl Into<BooleanBuffer>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Blob {
			container: blob_array(&data),
			max_bytes: MaxBytes::MAX,
		};
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn identity_id(identity_ids: impl IntoIterator<Item = IdentityId>) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		ColumnBuffer::IdentityId(identity_id_array(data))
	}

	pub(crate) fn identity_id_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::IdentityId(uuid_array::from_buffer(MutableBuffer::with_capacity(capacity * UUID_WIDTH)))
	}

	pub fn identity_id_with_bitvec(
		identity_ids: impl IntoIterator<Item = IdentityId>,
		bitvec: impl Into<BooleanBuffer>,
	) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::IdentityId(identity_id_array(data));
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn decimal(precision: Precision, scale: Scale, data: impl IntoIterator<Item = Decimal>) -> Self {
		ColumnBuffer::Decimal(decimal_array(precision, scale, data))
	}

	pub(crate) fn decimal_with_capacity(precision: Precision, scale: Scale, capacity: usize) -> Self {
		ColumnBuffer::Decimal(DecimalArray::from_unscaled(precision, scale, Vec::with_capacity(capacity)))
	}

	pub fn decimal_with_bitvec(
		precision: Precision,
		scale: Scale,
		data: impl IntoIterator<Item = Decimal>,
		bitvec: impl Into<BooleanBuffer>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Decimal(decimal_array(precision, scale, &data));
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn any(data: impl IntoIterator<Item = Value>) -> Self {
		ColumnBuffer::Any {
			container: any_array(data),
			declared_type: None,
		}
	}

	pub fn any_typed(data: impl IntoIterator<Item = Value>, declared_type: ValueType) -> Self {
		ColumnBuffer::Any {
			container: any_array(data),
			declared_type: Some(declared_type),
		}
	}

	pub(crate) fn any_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::Any {
			container: LargeBinaryBuilder::with_capacity(capacity, 0).finish(),
			declared_type: None,
		}
	}

	pub(crate) fn any_with_capacity_typed(capacity: usize, declared_type: ValueType) -> Self {
		ColumnBuffer::Any {
			container: LargeBinaryBuilder::with_capacity(capacity, 0).finish(),
			declared_type: Some(declared_type),
		}
	}

	pub fn any_with_bitvec(data: impl IntoIterator<Item = Value>, bitvec: impl Into<BooleanBuffer>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Any {
			container: any_array(&data),
			declared_type: None,
		};
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn any_with_bitvec_typed(
		data: impl IntoIterator<Item = Value>,
		bitvec: impl Into<BooleanBuffer>,
		declared_type: ValueType,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::Any {
			container: any_array(&data),
			declared_type: Some(declared_type),
		};
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn dictionary_id(data: impl IntoIterator<Item = DictionaryEntryId>) -> Self {
		ColumnBuffer::DictionaryId {
			container: dictionary_array(data),
			dictionary_id: None,
		}
	}

	pub(crate) fn dictionary_id_with_capacity(capacity: usize) -> Self {
		ColumnBuffer::DictionaryId {
			container: dictionary_array::from_buffer(MutableBuffer::with_capacity(
				capacity * DICTIONARY_ENTRY_WIDTH,
			)),
			dictionary_id: None,
		}
	}

	pub fn dictionary_id_with_bitvec(
		data: impl IntoIterator<Item = DictionaryEntryId>,
		bitvec: impl Into<BooleanBuffer>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnBuffer::DictionaryId {
			container: dictionary_array(data),
			dictionary_id: None,
		};
		if !bitvec.has_false() {
			inner
		} else {
			inner.with_nulls(NullBuffer::new(bitvec))
		}
	}

	pub fn typed_none(ty: &ValueType) -> Self {
		match ty {
			ValueType::Option(inner) => Self::typed_none(inner),
			_ => Self::none_typed(ty.clone(), 1),
		}
	}

	pub fn none_typed(ty: ValueType, len: usize) -> Self {
		let bitvec = BooleanBuffer::new_unset(len);
		let inner = match ty {
			ValueType::Boolean => Self::bool(vec![false; len]),
			ValueType::Float4 => Self::float4(vec![0.0f32; len]),
			ValueType::Float8 => Self::float8(vec![0.0f64; len]),
			ValueType::Int1 => Self::int1(vec![0i8; len]),
			ValueType::Int2 => Self::int2(vec![0i16; len]),
			ValueType::Int4 => Self::int4(vec![0i32; len]),
			ValueType::Int8 => Self::int8(vec![0i64; len]),
			ValueType::Int16 => Self::int16(vec![0i128; len]),
			ValueType::Utf8 => Self::utf8(vec![String::new(); len]),
			ValueType::Uint1 => Self::uint1(vec![0u8; len]),
			ValueType::Uint2 => Self::uint2(vec![0u16; len]),
			ValueType::Uint4 => Self::uint4(vec![0u32; len]),
			ValueType::Uint8 => Self::uint8(vec![0u64; len]),
			ValueType::Uint16 => Self::uint16(vec![0u128; len]),
			ValueType::Date => Self::date(vec![Date::default(); len]),
			ValueType::DateTime => Self::datetime(vec![DateTime::default(); len]),
			ValueType::Time => Self::time(vec![Time::default(); len]),
			ValueType::Duration => Self::duration(vec![Duration::default(); len]),
			ValueType::Blob => Self::blob(vec![Blob::new(vec![]); len]),
			ValueType::Uuid4 => Self::uuid4(vec![Uuid4::default(); len]),
			ValueType::Uuid7 => Self::uuid7(vec![Uuid7::default(); len]),
			ValueType::IdentityId => Self::identity_id(vec![IdentityId::default(); len]),
			ValueType::Decimal {
				precision,
				scale,
			} => Self::decimal(precision, scale, vec![Decimal::default(); len]),
			ValueType::Any => Self::any(vec![Value::none(); len]),
			ValueType::DictionaryId => Self::dictionary_id(vec![DictionaryEntryId::default(); len]),
			list_ty @ ValueType::List(_) => Self::any_typed(vec![Value::List(vec![]); len], list_ty),
			record_ty @ ValueType::Record(_) => {
				Self::any_typed(vec![Value::Record(vec![]); len], record_ty)
			}
			ValueType::Tuple(_) => Self::any(vec![Value::Tuple(vec![]); len]),
			ValueType::Option(inner) => return Self::none_typed(*inner, len),
			ValueType::Digest {
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: digest_array((0..len).map(|_| None::<Digest>)),
				inner: *inner,
				accuracy,
			},
		};
		inner.with_nulls(NullBuffer::new(bitvec))
	}
}

#[cfg(test)]
mod tests {
	use arrow_schema::{DataType, TimeUnit};
	use reifydb_value::{
		util::kernel,
		value::{datetime::DateTime, value_type::ValueType},
	};

	use super::*;
	use crate::value::column::{buffer::take::as_array, builder::ColumnBuilder};

	fn expected_datetime_type() -> DataType {
		DataType::Timestamp(TimeUnit::Nanosecond, Some(DATETIME_TIMEZONE.into()))
	}

	#[test]
	fn every_datetime_construction_path_agrees_on_the_arrow_data_type() {
		// A path that drops the "+00:00" timezone cannot interleave with the others, which require exact
		// DataType equality.
		let via_data = ColumnBuffer::datetime(vec![DateTime::from_nanos(0)]);
		let via_capacity = ColumnBuffer::datetime_with_capacity(1);
		let via_builder = ColumnBuilder::with_capacity(ValueType::DateTime, 1).finish();

		assert_eq!(as_array(&via_data).data_type(), &expected_datetime_type());
		assert_eq!(as_array(&via_capacity).data_type(), &expected_datetime_type());
		assert_eq!(as_array(&via_builder).data_type(), &expected_datetime_type());
	}

	#[test]
	fn interleaving_datetime_buffers_from_different_construction_paths_does_not_panic() {
		let source = ColumnBuffer::datetime(vec![DateTime::from_nanos(1), DateTime::from_nanos(2)]);
		let filler = ColumnBuffer::datetime(vec![DateTime::from_nanos(0)]);

		let ColumnBuffer::DateTime(source_array) = &source else {
			panic!("expected a DateTime buffer");
		};
		let interleaved = kernel::interleaved(source_array, as_array(&filler), &[(0, 1), (1, 0)]);

		assert_eq!(interleaved.len(), 2);
	}
}
