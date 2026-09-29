// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	Array, ArrayRef, BooleanArray, LargeStringArray, NullArray, PrimitiveArray,
	types::{
		Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
		UInt64Type,
	},
};
use arrow_buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow_schema::FieldRef;
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	container::{
		any_array::{any_array, any_array_optional},
		bool_array,
		decimal_array::decimal_array,
		dictionary_array::dictionary_array,
		digest_array::digest_array,
		fixed_array, primitive,
		temporal_array::{date_array, datetime_array, duration_array, time_array},
		uuid_array::{identity_id_array, uuid4_array, uuid7_array},
		varlen_array::{self, blob_array},
		wide_int_array::wide_array,
	},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};

fn column(name: &str, value_type: ValueType, array: ArrayRef) -> (FieldRef, ArrayRef) {
	let value_type = match array.null_count() > 0 {
		true => ValueType::Option(Box::new(value_type)),
		false => value_type,
	};
	named(
		name,
		FieldType {
			value_type: Some(value_type),
			..FieldType::default()
		},
		array,
	)
}

fn validity(len: usize, bitvec: impl Into<BooleanBuffer>) -> Option<NullBuffer> {
	let bitvec = bitvec.into();
	assert_eq!(bitvec.len(), len);
	bitvec.has_false().then(|| NullBuffer::new(bitvec))
}

fn native<T>(values: Vec<T::Native>, nulls: Option<NullBuffer>) -> ArrayRef
where
	T: arrow_array::ArrowPrimitiveType,
{
	Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls))
}

macro_rules! native_factory {
	($name:ident, $name_bv:ident, $arrow:ty, $t:ty, $value_type:expr) => {
		pub fn $name(name: &str, data: impl IntoIterator<Item = $t>) -> (FieldRef, ArrayRef) {
			let values = data.into_iter().collect::<Vec<_>>();
			column(name, $value_type, native::<$arrow>(values, None))
		}

		pub fn $name_bv(
			name: &str,
			data: impl IntoIterator<Item = $t>,
			bitvec: impl Into<BooleanBuffer>,
		) -> (FieldRef, ArrayRef) {
			let values = data.into_iter().collect::<Vec<_>>();
			let nulls = validity(values.len(), bitvec);
			column(name, $value_type, native::<$arrow>(values, nulls))
		}
	};
}

macro_rules! built_factory {
	($name:ident, $name_bv:ident, $t:ty, $value_type:expr, $build:expr, $attach:path) => {
		pub fn $name(name: &str, data: impl IntoIterator<Item = $t>) -> (FieldRef, ArrayRef) {
			let values = data.into_iter().collect::<Vec<_>>();
			column(name, $value_type, Arc::new($build(values)))
		}

		pub fn $name_bv(
			name: &str,
			data: impl IntoIterator<Item = $t>,
			bitvec: impl Into<BooleanBuffer>,
		) -> (FieldRef, ArrayRef) {
			let values = data.into_iter().collect::<Vec<_>>();
			let nulls = validity(values.len(), bitvec);
			column(name, $value_type, Arc::new($attach($build(values), nulls)))
		}
	};
}

pub fn bool(name: &str, data: impl IntoIterator<Item = bool>) -> (FieldRef, ArrayRef) {
	let values = data.into_iter().collect::<Vec<_>>();
	column(name, ValueType::Boolean, Arc::new(BooleanArray::from(values)))
}

pub fn bool_with_bitvec(
	name: &str,
	data: impl IntoIterator<Item = bool>,
	bitvec: impl Into<BooleanBuffer>,
) -> (FieldRef, ArrayRef) {
	let values = data.into_iter().collect::<Vec<_>>();
	let nulls = validity(values.len(), bitvec);
	column(name, ValueType::Boolean, Arc::new(bool_array::attach_nulls(BooleanArray::from(values), nulls)))
}

native_factory!(float4, float4_with_bitvec, Float32Type, f32, ValueType::Float4);
native_factory!(float8, float8_with_bitvec, Float64Type, f64, ValueType::Float8);
native_factory!(int1, int1_with_bitvec, Int8Type, i8, ValueType::Int1);
native_factory!(int2, int2_with_bitvec, Int16Type, i16, ValueType::Int2);
native_factory!(int4, int4_with_bitvec, Int32Type, i32, ValueType::Int4);
native_factory!(int8, int8_with_bitvec, Int64Type, i64, ValueType::Int8);
native_factory!(uint1, uint1_with_bitvec, UInt8Type, u8, ValueType::Uint1);
native_factory!(uint2, uint2_with_bitvec, UInt16Type, u16, ValueType::Uint2);
native_factory!(uint4, uint4_with_bitvec, UInt32Type, u32, ValueType::Uint4);
native_factory!(uint8, uint8_with_bitvec, UInt64Type, u64, ValueType::Uint8);

built_factory!(int16, int16_with_bitvec, i128, ValueType::Int16, wide_array, fixed_array::attach_nulls);
built_factory!(uint16, uint16_with_bitvec, u128, ValueType::Uint16, wide_array, fixed_array::attach_nulls);
built_factory!(date, date_with_bitvec, Date, ValueType::Date, date_array, primitive::attach_nulls);
built_factory!(datetime, datetime_with_bitvec, DateTime, ValueType::DateTime, datetime_array, primitive::attach_nulls);
built_factory!(time, time_with_bitvec, Time, ValueType::Time, time_array, primitive::attach_nulls);
built_factory!(duration, duration_with_bitvec, Duration, ValueType::Duration, duration_array, primitive::attach_nulls);
built_factory!(uuid4, uuid4_with_bitvec, Uuid4, ValueType::Uuid4, uuid4_array, fixed_array::attach_nulls);
built_factory!(uuid7, uuid7_with_bitvec, Uuid7, ValueType::Uuid7, uuid7_array, fixed_array::attach_nulls);
built_factory!(
	identity_id,
	identity_id_with_bitvec,
	IdentityId,
	ValueType::IdentityId,
	identity_id_array,
	fixed_array::attach_nulls
);
built_factory!(
	dictionary_id,
	dictionary_id_with_bitvec,
	DictionaryEntryId,
	ValueType::DictionaryId,
	dictionary_array,
	fixed_array::attach_nulls
);
built_factory!(
	blob,
	blob_with_bitvec,
	Blob,
	ValueType::Blob,
	|values: Vec<Blob>| blob_array(&values),
	varlen_array::attach_nulls
);

pub fn int4_optional(name: &str, data: impl IntoIterator<Item = Option<i32>>) -> (FieldRef, ArrayRef) {
	let (values, valid): (Vec<i32>, Vec<bool>) =
		data.into_iter().map(|value| (value.unwrap_or_default(), value.is_some())).unzip();
	let nulls = validity(values.len(), BooleanBuffer::from(valid));
	column(name, ValueType::Int4, native::<Int32Type>(values, nulls))
}

pub fn utf8(name: &str, data: impl IntoIterator<Item = impl Into<String>>) -> (FieldRef, ArrayRef) {
	let values = data.into_iter().map(Into::into).collect::<Vec<String>>();
	column(name, ValueType::Utf8, Arc::new(LargeStringArray::from(values)))
}

pub fn utf8_repeated(name: &str, value: &str, count: usize) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Utf8, Arc::new(LargeStringArray::new_repeated(value, count)))
}

pub fn utf8_with_bitvec(
	name: &str,
	data: impl IntoIterator<Item = impl Into<String>>,
	bitvec: impl Into<BooleanBuffer>,
) -> (FieldRef, ArrayRef) {
	let values = data.into_iter().map(Into::into).collect::<Vec<String>>();
	let nulls = validity(values.len(), bitvec);
	column(name, ValueType::Utf8, Arc::new(varlen_array::attach_nulls(LargeStringArray::from(values), nulls)))
}

pub fn decimal(
	name: &str,
	precision: Precision,
	scale: Scale,
	data: impl IntoIterator<Item = Decimal>,
) -> (FieldRef, ArrayRef) {
	let array = decimal_array(precision, scale, data);
	let value_type = ValueType::decimal(array.precision(), array.scale());
	column(name, value_type, array.into_array())
}

pub fn decimal_with_bitvec(
	name: &str,
	precision: Precision,
	scale: Scale,
	data: impl IntoIterator<Item = Decimal>,
	bitvec: impl Into<BooleanBuffer>,
) -> (FieldRef, ArrayRef) {
	let values = data.into_iter().collect::<Vec<_>>();
	let nulls = validity(values.len(), bitvec);
	let array = decimal_array(precision, scale, &values);
	let value_type = ValueType::decimal(array.precision(), array.scale());
	let array = array.into_array();
	let array = match nulls {
		Some(nulls) => with_validity(array, nulls),
		None => array,
	};
	column(name, value_type, array)
}

pub fn any(name: &str, data: impl IntoIterator<Item = Value>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Any, Arc::new(any_array(data)))
}

pub fn any_typed(name: &str, data: impl IntoIterator<Item = Value>, declared_type: ValueType) -> (FieldRef, ArrayRef) {
	declared(name, Arc::new(any_array(data)), declared_type)
}

pub fn any_optional(name: &str, data: impl IntoIterator<Item = Option<Value>>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Any, Arc::new(any_array_optional(data)))
}

pub fn any_optional_typed(
	name: &str,
	data: impl IntoIterator<Item = Option<Value>>,
	declared_type: ValueType,
) -> (FieldRef, ArrayRef) {
	declared(name, Arc::new(any_array_optional(data)), declared_type)
}

fn declared(name: &str, array: ArrayRef, declared_type: ValueType) -> (FieldRef, ArrayRef) {
	let value_type = match array.null_count() > 0 {
		true => ValueType::Option(Box::new(declared_type.clone())),
		false => declared_type.clone(),
	};
	named(
		name,
		FieldType {
			value_type: Some(value_type),
			declared_type: Some(declared_type),
			..FieldType::default()
		},
		array,
	)
}

pub fn none(name: &str, len: usize) -> (FieldRef, ArrayRef) {
	named(name, FieldType::default(), Arc::new(NullArray::new(len)))
}

pub fn typed_none(name: &str, ty: &ValueType) -> (FieldRef, ArrayRef) {
	match ty {
		ValueType::Option(inner) => typed_none(name, inner),
		_ => none_typed(name, ty.clone(), 1),
	}
}

pub fn none_typed(name: &str, ty: ValueType, len: usize) -> (FieldRef, ArrayRef) {
	let (field, array) = match ty {
		ValueType::Boolean => bool(name, vec![false; len]),
		ValueType::Float4 => float4(name, vec![0.0f32; len]),
		ValueType::Float8 => float8(name, vec![0.0f64; len]),
		ValueType::Int1 => int1(name, vec![0i8; len]),
		ValueType::Int2 => int2(name, vec![0i16; len]),
		ValueType::Int4 => int4(name, vec![0i32; len]),
		ValueType::Int8 => int8(name, vec![0i64; len]),
		ValueType::Int16 => int16(name, vec![0i128; len]),
		ValueType::Utf8 => utf8(name, vec![String::new(); len]),
		ValueType::Uint1 => uint1(name, vec![0u8; len]),
		ValueType::Uint2 => uint2(name, vec![0u16; len]),
		ValueType::Uint4 => uint4(name, vec![0u32; len]),
		ValueType::Uint8 => uint8(name, vec![0u64; len]),
		ValueType::Uint16 => uint16(name, vec![0u128; len]),
		ValueType::Date => date(name, vec![Date::default(); len]),
		ValueType::DateTime => datetime(name, vec![DateTime::default(); len]),
		ValueType::Time => time(name, vec![Time::default(); len]),
		ValueType::Duration => duration(name, vec![Duration::default(); len]),
		ValueType::Blob => blob(name, vec![Blob::new(vec![]); len]),
		ValueType::Uuid4 => uuid4(name, vec![Uuid4::default(); len]),
		ValueType::Uuid7 => uuid7(name, vec![Uuid7::default(); len]),
		ValueType::IdentityId => identity_id(name, vec![IdentityId::default(); len]),
		ValueType::Decimal {
			precision,
			scale,
		} => decimal(name, precision, scale, vec![Decimal::default(); len]),
		ValueType::Any => any_optional(name, vec![None; len]),
		ValueType::DictionaryId => dictionary_id(name, vec![DictionaryEntryId::default(); len]),
		list_ty @ ValueType::List(_) => any_typed(name, vec![Value::List(vec![]); len], list_ty),
		record_ty @ ValueType::Record(_) => any_typed(name, vec![Value::Record(vec![]); len], record_ty),
		ValueType::Tuple(_) => any(name, vec![Value::Tuple(vec![]); len]),
		ValueType::Option(inner) => return none_typed(name, *inner, len),
		ValueType::Digest {
			inner,
			accuracy,
		} => named(
			name,
			FieldType {
				value_type: Some(ValueType::Digest {
					inner,
					accuracy,
				}),
				..FieldType::default()
			},
			Arc::new(digest_array((0..len).map(|_| None::<reifydb_value::value::digest::Digest>))),
		),
	};
	let array = with_validity(array, NullBuffer::new_null(len));
	(Arc::new(field.as_ref().clone().with_nullable(true)), array)
}

pub fn from_many(name: &str, value: Value, row_count: usize) -> (FieldRef, ArrayRef) {
	match value {
		Value::Boolean(v) => bool(name, vec![v; row_count]),
		Value::Float4(v) => float4(name, vec![v.value(); row_count]),
		Value::Float8(v) => float8(name, vec![v.value(); row_count]),
		Value::Int1(v) => int1(name, vec![v; row_count]),
		Value::Int2(v) => int2(name, vec![v; row_count]),
		Value::Int4(v) => int4(name, vec![v; row_count]),
		Value::Int8(v) => int8(name, vec![v; row_count]),
		Value::Int16(v) => int16(name, vec![v; row_count]),
		Value::Utf8(v) => utf8(name, vec![v; row_count]),
		Value::Uint1(v) => uint1(name, vec![v; row_count]),
		Value::Uint2(v) => uint2(name, vec![v; row_count]),
		Value::Uint4(v) => uint4(name, vec![v; row_count]),
		Value::Uint8(v) => uint8(name, vec![v; row_count]),
		Value::Uint16(v) => uint16(name, vec![v; row_count]),
		Value::Date(v) => date(name, vec![v; row_count]),
		Value::DateTime(v) => datetime(name, vec![v; row_count]),
		Value::Time(v) => time(name, vec![v; row_count]),
		Value::Duration(v) => duration(name, vec![v; row_count]),
		Value::IdentityId(v) => identity_id(name, vec![v; row_count]),
		Value::Uuid4(v) => uuid4(name, vec![v; row_count]),
		Value::Uuid7(v) => uuid7(name, vec![v; row_count]),
		Value::Blob(v) => blob(name, vec![v; row_count]),
		Value::Decimal(v) => decimal(name, Precision::MAX, Scale::new(v.scale()), vec![v; row_count]),
		Value::DictionaryId(v) => dictionary_id(name, vec![v; row_count]),
		Value::None {
			inner: ValueType::Any,
		} => none(name, row_count),
		Value::None {
			inner,
		} => none_typed(name, inner, row_count),
		Value::Type(t) => any(name, vec![Value::Type(t); row_count]),
		Value::Any(v) if matches!(*v, Value::None { .. }) => any_optional(name, vec![None; row_count]),
		Value::Any(v) => any(name, vec![*v; row_count]),
		Value::List(v) => any(name, vec![Value::List(v); row_count]),
		Value::Record(v) => any(name, vec![Value::Record(v); row_count]),
		Value::Tuple(v) => any(name, vec![Value::Tuple(v); row_count]),
		Value::Digest(digest) => named(
			name,
			FieldType {
				value_type: Some(ValueType::Digest {
					inner: Box::new(digest.inner().clone()),
					accuracy: digest.accuracy(),
				}),
				..FieldType::default()
			},
			Arc::new(digest_array((0..row_count).map(|_| Some(digest.as_ref())))),
		),
	}
}

pub fn rename(column: (FieldRef, ArrayRef), name: &str) -> (FieldRef, ArrayRef) {
	let (field, array) = column;
	(Arc::new(field.as_ref().clone().with_name(name)), array)
}

pub(crate) fn with_validity(array: ArrayRef, nulls: NullBuffer) -> ArrayRef {
	if array.as_any().is::<NullArray>() {
		return array;
	}
	let data = array
		.to_data()
		.into_builder()
		.nulls(Some(nulls))
		.build()
		.expect("a validity buffer of the array length always attaches");
	arrow_array::make_array(data)
}

#[cfg(test)]
mod tests {
	use arrow_array::{Array, TimestampNanosecondArray};
	use arrow_schema::{DataType, TimeUnit};
	use reifydb_value::{
		util::kernel,
		value::{
			Value, column_view::ColumnView, container::temporal_array::DATETIME_TIMEZONE,
			datetime::DateTime, ordered_f32::OrderedF32, ordered_f64::OrderedF64, value_type::ValueType,
		},
	};

	use super::*;
	use crate::value::column::builder::ColumnBuilder;

	fn expected_datetime_type() -> DataType {
		DataType::Timestamp(TimeUnit::Nanosecond, Some(DATETIME_TIMEZONE.into()))
	}

	#[test]
	fn every_datetime_construction_path_agrees_on_the_arrow_data_type() {
		// A path that drops the "+00:00" timezone cannot interleave with the others, which require exact
		// DataType equality.
		let (_, via_data) = datetime("c", vec![DateTime::from_nanos(0)]);
		let (_, via_builder) = ColumnBuilder::with_capacity(ValueType::DateTime, 1).finish("c");

		assert_eq!(via_data.data_type(), &expected_datetime_type());
		assert_eq!(via_builder.data_type(), &expected_datetime_type());
	}

	#[test]
	fn interleaving_datetime_buffers_from_different_construction_paths_does_not_panic() {
		let (_, source) = datetime("c", vec![DateTime::from_nanos(1), DateTime::from_nanos(2)]);
		let (_, filler) = datetime("c", vec![DateTime::from_nanos(0)]);

		let source_array = source.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
		let interleaved = kernel::interleaved(source_array, filler.as_ref(), &[(0, 1), (1, 0)]);

		assert_eq!(interleaved.len(), 2);
	}

	#[test]
	fn test_from_many_float4_repeats_for_every_row() {
		let col = from_many("c", Value::Float4(OrderedF32::try_from(1.5).unwrap()), 3);
		let col = ColumnView::try_from(&col).unwrap();
		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(2), Value::Float4(OrderedF32::try_from(1.5).unwrap()));
	}

	#[test]
	fn test_from_many_float8_repeats_for_every_row() {
		let col = from_many("c", Value::Float8(OrderedF64::try_from(2.5).unwrap()), 3);
		let col = ColumnView::try_from(&col).unwrap();
		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(2), Value::Float8(OrderedF64::try_from(2.5).unwrap()));
	}
}
