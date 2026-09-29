// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::type_name, result::Result as StdResult};

use arrow_array::{
	Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
	Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
	LargeBinaryArray, LargeStringArray, NullArray, Time64NanosecondArray, TimestampNanosecondArray, UInt8Array,
	UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, FieldRef};

use crate::{
	Result,
	error::{ColumnReadReason, Error, TypeError},
	util::float_format::{format_f32, format_f64},
	value::{
		Value,
		constraint::bytes::MaxBytes,
		container::{
			any_array, bool_array,
			decimal_array::{DecimalView, decimal_as_string, decimal_at, decimal_get_value},
			dictionary_array, digest_array, primitive,
			temporal_array::{self, dates, datetimes, durations, times},
			uuid_array::{self, identity_ids, uuid4s, uuid7s},
			varlen_array::{self, blob_as_string, blob_get_value, utf8_as_string, utf8_get_value},
			wide_int_array::{self, wide_at},
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryId,
		duration::Duration,
		identity::IdentityId,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{field_error, from_field},
		},
	},
};

#[derive(Clone, Debug)]
pub enum ViewData<'a> {
	Bool(&'a BooleanArray),
	Float4(&'a Float32Array),
	Float8(&'a Float64Array),
	Int1(&'a Int8Array),
	Int2(&'a Int16Array),
	Int4(&'a Int32Array),
	Int8(&'a Int64Array),
	Int16(&'a FixedSizeBinaryArray),
	Uint1(&'a UInt8Array),
	Uint2(&'a UInt16Array),
	Uint4(&'a UInt32Array),
	Uint8(&'a UInt64Array),
	Uint16(&'a FixedSizeBinaryArray),
	Utf8 {
		container: &'a LargeStringArray,
		max_bytes: MaxBytes,
	},
	Date(&'a Date32Array),
	DateTime(&'a TimestampNanosecondArray),
	Time(&'a Time64NanosecondArray),
	Duration(&'a IntervalMonthDayNanoArray),
	IdentityId(&'a FixedSizeBinaryArray),
	Uuid4(&'a FixedSizeBinaryArray),
	Uuid7(&'a FixedSizeBinaryArray),
	Blob {
		container: &'a LargeBinaryArray,
		max_bytes: MaxBytes,
	},
	Decimal(DecimalView<'a>),
	Any {
		container: &'a LargeBinaryArray,
		declared_type: Option<ValueType>,
	},
	DictionaryId {
		container: &'a FixedSizeBinaryArray,
		dictionary_id: Option<DictionaryId>,
	},
	Digest {
		container: &'a LargeBinaryArray,
		inner: ValueType,
		accuracy: u32,
	},
	None {
		array: &'a NullArray,
	},
}

#[derive(Clone, Debug)]
pub struct ColumnView<'a> {
	pub data: ViewData<'a>,
	pub field: &'a Field,
}

impl<'a> TryFrom<(&'a ArrayRef, &'a Field)> for ColumnView<'a> {
	type Error = Error;

	fn try_from((array, field): (&'a ArrayRef, &'a Field)) -> Result<Self> {
		if array.data_type() != field.data_type() {
			return Err(field_error(format!(
				"column {} holds arrow type {}, but its field says {}",
				field.name(),
				array.data_type(),
				field.data_type()
			)));
		}
		Ok(ColumnView {
			data: view_data(array, field)?,
			field,
		})
	}
}

impl<'a> TryFrom<&'a (FieldRef, ArrayRef)> for ColumnView<'a> {
	type Error = Error;

	fn try_from((field, array): &'a (FieldRef, ArrayRef)) -> Result<Self> {
		ColumnView::try_from((array, field.as_ref()))
	}
}

fn view_data<'a>(array: &'a ArrayRef, field: &'a Field) -> Result<ViewData<'a>> {
	let field_type = from_field(field)?;
	let Some(value_type) = field_type.value_type else {
		return Ok(ViewData::None {
			array: downcast(array, field)?,
		});
	};
	let bare = match value_type {
		ValueType::Option(inner) => *inner,
		other => other,
	};
	Ok(match bare {
		ValueType::Boolean => ViewData::Bool(downcast(array, field)?),
		ValueType::Float4 => ViewData::Float4(downcast(array, field)?),
		ValueType::Float8 => ViewData::Float8(downcast(array, field)?),
		ValueType::Int1 => ViewData::Int1(downcast(array, field)?),
		ValueType::Int2 => ViewData::Int2(downcast(array, field)?),
		ValueType::Int4 => ViewData::Int4(downcast(array, field)?),
		ValueType::Int8 => ViewData::Int8(downcast(array, field)?),
		ValueType::Int16 => ViewData::Int16(downcast(array, field)?),
		ValueType::Uint1 => ViewData::Uint1(downcast(array, field)?),
		ValueType::Uint2 => ViewData::Uint2(downcast(array, field)?),
		ValueType::Uint4 => ViewData::Uint4(downcast(array, field)?),
		ValueType::Uint8 => ViewData::Uint8(downcast(array, field)?),
		ValueType::Uint16 => ViewData::Uint16(downcast(array, field)?),
		ValueType::Utf8 => ViewData::Utf8 {
			container: downcast(array, field)?,
			max_bytes: field_type.max_bytes.unwrap_or(MaxBytes::MAX),
		},
		ValueType::Date => ViewData::Date(downcast(array, field)?),
		ValueType::DateTime => ViewData::DateTime(downcast(array, field)?),
		ValueType::Time => ViewData::Time(downcast(array, field)?),
		ValueType::Duration => ViewData::Duration(downcast(array, field)?),
		ValueType::IdentityId => ViewData::IdentityId(downcast(array, field)?),
		ValueType::Uuid4 => ViewData::Uuid4(downcast(array, field)?),
		ValueType::Uuid7 => ViewData::Uuid7(downcast(array, field)?),
		ValueType::Blob => ViewData::Blob {
			container: downcast(array, field)?,
			max_bytes: field_type.max_bytes.unwrap_or(MaxBytes::MAX),
		},
		ValueType::Decimal {
			..
		} => match array.as_any().downcast_ref::<Decimal128Array>() {
			Some(container) => ViewData::Decimal(DecimalView::Decimal128(container)),
			None => ViewData::Decimal(DecimalView::Decimal256(downcast::<Decimal256Array>(array, field)?)),
		},
		ValueType::Any | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_) => ViewData::Any {
			container: downcast(array, field)?,
			declared_type: field_type.declared_type,
		},
		ValueType::DictionaryId => ViewData::DictionaryId {
			container: downcast(array, field)?,
			dictionary_id: field_type.dictionary_id,
		},
		ValueType::Digest {
			inner,
			accuracy,
		} => ViewData::Digest {
			container: downcast(array, field)?,
			inner: *inner,
			accuracy,
		},
		ValueType::Option(_) => {
			return Err(field_error(format!("column {} has a nested Option type", field.name())));
		}
	})
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, field: &Field) -> Result<&'a T> {
	array.as_any().downcast_ref::<T>().ok_or_else(|| {
		field_error(format!(
			"column {} holds arrow type {}, which is not a {}",
			field.name(),
			array.data_type(),
			type_name::<T>()
		))
	})
}

pub trait FromColumnView: Sized {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason>;
}

pub trait AsSlice<'a, T> {
	fn as_slice(&self) -> &'a [T];
}

impl<'a> ColumnView<'a> {
	pub fn array(&self) -> &'a dyn Array {
		match &self.data {
			ViewData::Bool(a) => *a,
			ViewData::Float4(a) => *a,
			ViewData::Float8(a) => *a,
			ViewData::Int1(a) => *a,
			ViewData::Int2(a) => *a,
			ViewData::Int4(a) => *a,
			ViewData::Int8(a) => *a,
			ViewData::Int16(a) => *a,
			ViewData::Uint1(a) => *a,
			ViewData::Uint2(a) => *a,
			ViewData::Uint4(a) => *a,
			ViewData::Uint8(a) => *a,
			ViewData::Uint16(a) => *a,
			ViewData::Utf8 {
				container,
				..
			} => *container,
			ViewData::Date(a) => *a,
			ViewData::DateTime(a) => *a,
			ViewData::Time(a) => *a,
			ViewData::Duration(a) => *a,
			ViewData::IdentityId(a) => *a,
			ViewData::Uuid4(a) => *a,
			ViewData::Uuid7(a) => *a,
			ViewData::Blob {
				container,
				..
			} => *container,
			ViewData::Decimal(d) => d.array(),
			ViewData::Any {
				container,
				..
			} => *container,
			ViewData::DictionaryId {
				container,
				..
			} => *container,
			ViewData::Digest {
				container,
				..
			} => *container,
			ViewData::None {
				array,
			} => *array,
		}
	}

	pub fn len(&self) -> usize {
		self.array().len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn is_nullable(&self) -> bool {
		self.field.is_nullable()
	}

	pub fn logical_nulls(&self) -> Option<NullBuffer> {
		self.array().logical_nulls()
	}

	pub fn none_count(&self) -> usize {
		self.array().logical_null_count()
	}

	pub fn none_at(&self, index: usize) -> bool {
		match &self.data {
			ViewData::None {
				..
			} => true,
			_ => self.logical_nulls().is_some_and(|nulls| !(index < nulls.len() && nulls.is_valid(index))),
		}
	}

	pub fn base_type(&self) -> ValueType {
		match &self.data {
			ViewData::Bool(_) => ValueType::Boolean,
			ViewData::Float4(_) => ValueType::Float4,
			ViewData::Float8(_) => ValueType::Float8,
			ViewData::Int1(_) => ValueType::Int1,
			ViewData::Int2(_) => ValueType::Int2,
			ViewData::Int4(_) => ValueType::Int4,
			ViewData::Int8(_) => ValueType::Int8,
			ViewData::Int16(_) => ValueType::Int16,
			ViewData::Uint1(_) => ValueType::Uint1,
			ViewData::Uint2(_) => ValueType::Uint2,
			ViewData::Uint4(_) => ValueType::Uint4,
			ViewData::Uint8(_) => ValueType::Uint8,
			ViewData::Uint16(_) => ValueType::Uint16,
			ViewData::Utf8 {
				..
			} => ValueType::Utf8,
			ViewData::Date(_) => ValueType::Date,
			ViewData::DateTime(_) => ValueType::DateTime,
			ViewData::Time(_) => ValueType::Time,
			ViewData::Duration(_) => ValueType::Duration,
			ViewData::IdentityId(_) => ValueType::IdentityId,
			ViewData::Uuid4(_) => ValueType::Uuid4,
			ViewData::Uuid7(_) => ValueType::Uuid7,
			ViewData::Blob {
				..
			} => ValueType::Blob,
			ViewData::Decimal(d) => ValueType::decimal(d.precision(), d.scale()),
			ViewData::Any {
				declared_type,
				..
			} => declared_type.clone().unwrap_or(ValueType::Any),
			ViewData::DictionaryId {
				..
			} => ValueType::DictionaryId,
			ViewData::Digest {
				inner,
				accuracy,
				..
			} => ValueType::Digest {
				inner: Box::new(inner.clone()),
				accuracy: *accuracy,
			},
			ViewData::None {
				..
			} => ValueType::Any,
		}
	}

	pub fn get_type(&self) -> ValueType {
		match self.is_nullable() {
			true => ValueType::Option(Box::new(self.base_type())),
			false => self.base_type(),
		}
	}

	pub fn is_defined(&self, index: usize) -> bool {
		if self.none_at(index) {
			return false;
		}
		match &self.data {
			ViewData::Digest {
				container,
				..
			} => digest_array::is_defined(container, index),
			ViewData::None {
				..
			} => false,
			_ => index < self.len(),
		}
	}

	pub fn is_none(&self) -> bool {
		matches!(self.data, ViewData::None { .. })
	}

	pub fn is_untyped_none(&self) -> bool {
		match &self.data {
			ViewData::None {
				..
			} => true,
			ViewData::Any {
				declared_type: None,
				..
			} => self.logical_nulls().is_some_and(|nulls| nulls.null_count() == nulls.len()),
			_ => false,
		}
	}

	pub fn is_bool(&self) -> bool {
		self.get_type() == ValueType::Boolean
	}

	pub fn is_utf8(&self) -> bool {
		self.get_type() == ValueType::Utf8
	}

	pub fn is_number(&self) -> bool {
		matches!(
			self.get_type(),
			ValueType::Float4
				| ValueType::Float8
				| ValueType::Int1
				| ValueType::Int2
				| ValueType::Int4
				| ValueType::Int8
				| ValueType::Int16
				| ValueType::Uint1
				| ValueType::Uint2
				| ValueType::Uint4
				| ValueType::Uint8
				| ValueType::Uint16
				| ValueType::Decimal { .. }
		)
	}

	pub fn is_text(&self) -> bool {
		self.get_type() == ValueType::Utf8
	}

	pub fn is_temporal(&self) -> bool {
		matches!(self.get_type(), ValueType::Date | ValueType::DateTime | ValueType::Time | ValueType::Duration)
	}

	pub fn is_uuid(&self) -> bool {
		matches!(self.get_type(), ValueType::Uuid4 | ValueType::Uuid7)
	}

	pub fn get_value(&self, index: usize) -> Value {
		if self.none_at(index) {
			return Value::None {
				inner: self.base_type(),
			};
		}
		match &self.data {
			ViewData::Bool(a) => bool_array::get_value(a, index),
			ViewData::Float4(a) => primitive::get_value(*a, index),
			ViewData::Float8(a) => primitive::get_value(*a, index),
			ViewData::Int1(a) => primitive::get_value(*a, index),
			ViewData::Int2(a) => primitive::get_value(*a, index),
			ViewData::Int4(a) => primitive::get_value(*a, index),
			ViewData::Int8(a) => primitive::get_value(*a, index),
			ViewData::Int16(a) => wide_int_array::get_value::<i128>(a, index),
			ViewData::Uint1(a) => primitive::get_value(*a, index),
			ViewData::Uint2(a) => primitive::get_value(*a, index),
			ViewData::Uint4(a) => primitive::get_value(*a, index),
			ViewData::Uint8(a) => primitive::get_value(*a, index),
			ViewData::Uint16(a) => wide_int_array::get_value::<u128>(a, index),
			ViewData::Utf8 {
				container,
				..
			} => utf8_get_value(container, index),
			ViewData::Date(a) => temporal_array::get_value(dates(a), index),
			ViewData::DateTime(a) => temporal_array::get_value(datetimes(a), index),
			ViewData::Time(a) => temporal_array::get_value(times(a), index),
			ViewData::Duration(a) => temporal_array::get_value(durations(a), index),
			ViewData::IdentityId(a) => uuid_array::identity_id_get_value(identity_ids(a), index),
			ViewData::Uuid4(a) => uuid_array::get_value(uuid4s(a), index),
			ViewData::Uuid7(a) => uuid_array::get_value(uuid7s(a), index),
			ViewData::Blob {
				container,
				..
			} => blob_get_value(container, index),
			ViewData::Decimal(d) => decimal_get_value(d, index),
			ViewData::Any {
				container,
				declared_type,
			} => any_array::get_value(container, declared_type.as_ref(), index),
			ViewData::DictionaryId {
				container,
				..
			} => dictionary_array::get_value(container, index),
			ViewData::Digest {
				container,
				..
			} => digest_array::get_value(container, index),
			ViewData::None {
				..
			} => Value::none(),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		if self.none_at(index) {
			return "none".to_string();
		}
		match &self.data {
			ViewData::Bool(a) => bool_array::as_string(a, index),
			ViewData::Float4(a) => {
				a.values().get(index).map_or_else(|| "none".to_string(), |&v| format_f32(v))
			}
			ViewData::Float8(a) => {
				a.values().get(index).map_or_else(|| "none".to_string(), |&v| format_f64(v))
			}
			ViewData::Int1(a) => primitive::as_string(*a, index),
			ViewData::Int2(a) => primitive::as_string(*a, index),
			ViewData::Int4(a) => primitive::as_string(*a, index),
			ViewData::Int8(a) => primitive::as_string(*a, index),
			ViewData::Int16(a) => wide_int_array::as_string::<i128>(a, index),
			ViewData::Uint1(a) => primitive::as_string(*a, index),
			ViewData::Uint2(a) => primitive::as_string(*a, index),
			ViewData::Uint4(a) => primitive::as_string(*a, index),
			ViewData::Uint8(a) => primitive::as_string(*a, index),
			ViewData::Uint16(a) => wide_int_array::as_string::<u128>(a, index),
			ViewData::Utf8 {
				container,
				..
			} => utf8_as_string(container, index),
			ViewData::Date(a) => temporal_array::as_string(dates(a), index),
			ViewData::DateTime(a) => temporal_array::as_string(datetimes(a), index),
			ViewData::Time(a) => temporal_array::as_string(times(a), index),
			ViewData::Duration(a) => temporal_array::as_string(durations(a), index),
			ViewData::IdentityId(a) => uuid_array::as_string(identity_ids(a), index),
			ViewData::Uuid4(a) => uuid_array::as_string(uuid4s(a), index),
			ViewData::Uuid7(a) => uuid_array::as_string(uuid7s(a), index),
			ViewData::Blob {
				container,
				..
			} => blob_as_string(container, index),
			ViewData::Decimal(d) => decimal_as_string(d, index),
			ViewData::Any {
				container,
				..
			} => any_array::as_string(container, index),
			ViewData::DictionaryId {
				container,
				..
			} => dictionary_array::as_string(container, index),
			ViewData::Digest {
				container,
				..
			} => digest_array::as_string(container, index),
			ViewData::None {
				..
			} => "none".to_string(),
		}
	}

	pub fn get_as<T: FromColumnView>(&self, index: usize) -> Result<Option<T>> {
		if self.none_at(index) {
			return Ok(None);
		}
		T::from_column_view(self, index).map_err(|reason| {
			TypeError::ColumnRead {
				column_type: self.base_type(),
				target: type_name::<T>(),
				reason,
			}
			.into()
		})
	}

	pub fn get_str(&self, index: usize) -> Option<&'a str> {
		match &self.data {
			ViewData::Utf8 {
				container,
				..
			} if !self.none_at(index) => varlen_array::get(*container, index),
			_ => None,
		}
	}

	pub fn get_bytes(&self, index: usize) -> Option<&'a [u8]> {
		match &self.data {
			ViewData::Blob {
				container,
				..
			} if !self.none_at(index) => varlen_array::get(*container, index),
			_ => None,
		}
	}

	pub fn as_slice<T>(&self) -> &'a [T]
	where
		Self: AsSlice<'a, T>,
	{
		<Self as AsSlice<'a, T>>::as_slice(self)
	}

	pub fn iter(&self) -> impl Iterator<Item = Value> + '_ {
		(0..self.len()).map(move |index| self.get_value(index))
	}
}

impl<'a> AsSlice<'a, bool> for ColumnView<'a> {
	fn as_slice(&self) -> &'a [bool] {
		match &self.data {
			ViewData::Bool(_) => {
				panic!("as_slice() is not supported for BooleanArray. Use to_vec() instead.")
			}
			_ => panic!("called `as_slice::<bool>()` on ColumnView::{:?}", self.get_type()),
		}
	}
}

macro_rules! impl_as_slice {
	($t:ty, $variant:ident native) => {
		impl<'a> AsSlice<'a, $t> for ColumnView<'a> {
			fn as_slice(&self) -> &'a [$t] {
				match &self.data {
					ViewData::$variant(array) => array.values(),
					_ => panic!(
						"called `as_slice::<{}>()` on ColumnView::{:?}",
						stringify!($t),
						self.get_type()
					),
				}
			}
		}
	};
	($t:ty, $variant:ident typed $typed:ident) => {
		impl<'a> AsSlice<'a, $t> for ColumnView<'a> {
			fn as_slice(&self) -> &'a [$t] {
				match &self.data {
					ViewData::$variant(array) => $typed(array),
					_ => panic!(
						"called `as_slice::<{}>()` on ColumnView::{:?}",
						stringify!($t),
						self.get_type()
					),
				}
			}
		}
	};
}

impl_as_slice!(f32, Float4 native);
impl_as_slice!(f64, Float8 native);
impl_as_slice!(i8, Int1 native);
impl_as_slice!(i16, Int2 native);
impl_as_slice!(i32, Int4 native);
impl_as_slice!(i64, Int8 native);
impl_as_slice!(u8, Uint1 native);
impl_as_slice!(u16, Uint2 native);
impl_as_slice!(u32, Uint4 native);
impl_as_slice!(u64, Uint8 native);
impl_as_slice!(Date, Date typed dates);
impl_as_slice!(DateTime, DateTime typed datetimes);
impl_as_slice!(Time, Time typed times);
impl_as_slice!(Duration, Duration typed durations);

fn wrong_type<T>() -> StdResult<Option<T>, ColumnReadReason> {
	Err(ColumnReadReason::WrongType)
}

macro_rules! impl_from_column_view_widening {
	($($t:ty => [$($variant:ident),*]),* $(,)?) => { $(
		impl FromColumnView for $t {
			fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
				match &view.data {
					$(ViewData::$variant(c) => Ok(c.values().get(index).map(|&v| <$t>::from(v))),)*
					_ => wrong_type(),
				}
			}
		}
	)* };
}

impl_from_column_view_widening!(
	i8 => [Int1],
	i16 => [Int1, Int2],
	i32 => [Int1, Int2, Int4],
	i64 => [Int1, Int2, Int4, Int8],
	u8 => [Uint1],
	u16 => [Uint1, Uint2],
	u32 => [Uint1, Uint2, Uint4],
	u64 => [Uint1, Uint2, Uint4, Uint8],
	f32 => [Float4],
	f64 => [Float4, Float8],
);

impl FromColumnView for i128 {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Int1(c) => Ok(c.values().get(index).map(|&v| i128::from(v))),
			ViewData::Int2(c) => Ok(c.values().get(index).map(|&v| i128::from(v))),
			ViewData::Int4(c) => Ok(c.values().get(index).map(|&v| i128::from(v))),
			ViewData::Int8(c) => Ok(c.values().get(index).map(|&v| i128::from(v))),
			ViewData::Int16(c) => Ok(wide_at::<i128>(c, index)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for u128 {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Uint1(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ViewData::Uint2(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ViewData::Uint4(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ViewData::Uint8(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ViewData::Uint16(c) => Ok(wide_at::<u128>(c, index)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for bool {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Bool(c) => Ok((index < c.len()).then(|| c.value(index))),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for String {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Utf8 {
				container,
				..
			} => Ok(varlen_array::get(*container, index).map(str::to_string)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Vec<u8> {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Blob {
				container,
				..
			} => Ok(varlen_array::get(*container, index).map(<[u8]>::to_vec)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Date {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Date(c) => Ok(dates(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for DateTime {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::DateTime(c) => Ok(datetimes(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Time {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Time(c) => Ok(times(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Duration {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Duration(c) => Ok(durations(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Uuid4 {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Uuid4(c) => Ok(uuid4s(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Uuid7 {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Uuid7(c) => Ok(uuid7s(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for Decimal {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::Decimal(d) => Ok(decimal_at(d, index)),
			ViewData::Float4(c) => c
				.values()
				.get(index)
				.map(|&v| Decimal::from_f32(v).ok_or(ColumnReadReason::DoesNotFit))
				.transpose(),
			ViewData::Float8(c) => c
				.values()
				.get(index)
				.map(|&v| Decimal::from_f64(v).ok_or(ColumnReadReason::DoesNotFit))
				.transpose(),
			_ => wrong_type(),
		}
	}
}

impl FromColumnView for IdentityId {
	fn from_column_view(view: &ColumnView<'_>, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match &view.data {
			ViewData::IdentityId(c) => Ok(identity_ids(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}
