// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use arrow_array::{
	Array, ArrayRef, ArrowPrimitiveType, FixedSizeBinaryArray, GenericByteArray, PrimitiveArray,
	builder::{
		ArrayBuilder, BooleanBuilder, FixedSizeBinaryBuilder, GenericByteBuilder, LargeBinaryBuilder,
		LargeStringBuilder, NullBuilder, PrimitiveBuilder,
	},
	types::{
		ByteArrayType, Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
		Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType, Time64NanosecondType,
		TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
	},
};
use arrow_buffer::{Buffer, NullBuffer, bit_util, i256};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::{
		column_view::{ColumnView, ViewData},
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			decimal_array::{self, DECIMAL128_MAX_PRECISION, DecimalArray, DecimalView, decimal_at},
			dictionary_array::DICTIONARY_ENTRY_WIDTH,
			temporal_array::datetime_timezone,
			uuid_array::UUID_WIDTH,
			varlen_array,
			wide_int_array::WideInt,
		},
		decimal::{Decimal, unscaled},
		dictionary::DictionaryId,
		value_type::{
			ValueType,
			field::{FieldType, named},
		},
	},
};

use crate::{internal_err, internal_error, value::column::push::Push};

#[derive(Debug)]
pub enum DecimalBuilder {
	Decimal128 {
		builder: PrimitiveBuilder<Decimal128Type>,
		precision: Precision,
		scale: Scale,
	},
	Decimal256 {
		builder: PrimitiveBuilder<Decimal256Type>,
		precision: Precision,
		scale: Scale,
	},
}

impl DecimalBuilder {
	pub fn with_capacity(precision: Precision, scale: Scale, capacity: usize) -> Self {
		let data_type = decimal_array::data_type(precision, scale);
		if precision.value() <= DECIMAL128_MAX_PRECISION {
			DecimalBuilder::Decimal128 {
				builder: PrimitiveBuilder::with_capacity(capacity).with_data_type(data_type),
				precision,
				scale,
			}
		} else {
			DecimalBuilder::Decimal256 {
				builder: PrimitiveBuilder::with_capacity(capacity).with_data_type(data_type),
				precision,
				scale,
			}
		}
	}

	pub(crate) fn from_array(array: DecimalArray) -> Self {
		let (precision, scale) = (array.precision(), array.scale());
		match array {
			DecimalArray::Decimal128(array) => DecimalBuilder::Decimal128 {
				builder: primitive_builder(array),
				precision,
				scale,
			},
			DecimalArray::Decimal256(array) => DecimalBuilder::Decimal256 {
				builder: primitive_builder(array),
				precision,
				scale,
			},
		}
	}

	pub fn precision(&self) -> Precision {
		match self {
			DecimalBuilder::Decimal128 {
				precision,
				..
			}
			| DecimalBuilder::Decimal256 {
				precision,
				..
			} => *precision,
		}
	}

	pub fn scale(&self) -> Scale {
		match self {
			DecimalBuilder::Decimal128 {
				scale,
				..
			}
			| DecimalBuilder::Decimal256 {
				scale,
				..
			} => *scale,
		}
	}

	pub fn len(&self) -> usize {
		match self {
			DecimalBuilder::Decimal128 {
				builder,
				..
			} => builder.len(),
			DecimalBuilder::Decimal256 {
				builder,
				..
			} => builder.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn finish(&mut self) -> DecimalArray {
		match self {
			DecimalBuilder::Decimal128 {
				builder,
				..
			} => DecimalArray::Decimal128(builder.finish()),
			DecimalBuilder::Decimal256 {
				builder,
				..
			} => DecimalArray::Decimal256(builder.finish()),
		}
	}

	pub(crate) fn append_null(&mut self) {
		match self {
			DecimalBuilder::Decimal128 {
				builder,
				..
			} => builder.append_null(),
			DecimalBuilder::Decimal256 {
				builder,
				..
			} => builder.append_null(),
		}
	}

	pub fn push(&mut self, value: &Decimal) {
		if let Some(fitted) = value.fits(self.precision().value(), self.scale().value()) {
			return self.append_fitting(fitted.unscaled());
		}
		let fitted = self.widen_for(value);
		self.append_fitting(fitted.unscaled());
	}

	pub(crate) fn append_array(&mut self, array: DecimalView<'_>) {
		match (self, array) {
			(
				DecimalBuilder::Decimal128 {
					builder,
					precision,
					scale,
				},
				DecimalView::Decimal128(array),
			) if array.precision() == precision.value() && array.scale() as u8 == scale.value() => builder.append_array(array),
			(
				DecimalBuilder::Decimal256 {
					builder,
					precision,
					scale,
				},
				DecimalView::Decimal256(array),
			) if array.precision() == precision.value() && array.scale() as u8 == scale.value() => builder.append_array(array),
			(this, array) => {
				let nulls = decimal_nulls(array);
				for index in 0..array.len() {
					if nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)) {
						this.append_null();
						continue;
					}
					let value = decimal_at(array, index).expect("index is below the array length");
					this.push(&value);
				}
			}
		}
	}

	fn append_fitting(&mut self, unscaled: i256) {
		match self {
			DecimalBuilder::Decimal128 {
				builder,
				..
			} => builder.append_value(unscaled.to_i128().expect("a value within precision 38 fits i128")),
			DecimalBuilder::Decimal256 {
				builder,
				..
			} => builder.append_value(unscaled),
		}
	}

	fn widen_for(&mut self, value: &Decimal) -> Decimal {
		let (precision, scale) = (self.precision().value(), self.scale().value());
		let finished = self.finish();
		let nulls = decimal_nulls(&finished);
		let existing: Vec<Decimal> = finished
			.unscaled_values()
			.into_iter()
			.map(|unscaled| {
				Decimal::from_parts(unscaled, scale)
					.expect("a decimal builder holds only valid decimals")
			})
			.collect();
		let integer_digits = |decimal: &Decimal| decimal.digits().saturating_sub(decimal.scale());
		let needed = existing.iter().chain([value]).map(integer_digits).max().unwrap_or(0);
		let wide_scale = scale.max(value.scale()).min(unscaled::MAX_DIGITS - needed);
		let round = |decimal: &Decimal| {
			decimal.round_to_scale(wide_scale)
				.expect("rounding never adds a whole digit past the widest value")
		};
		let rounded: Vec<Decimal> = existing.iter().map(round).collect();
		let fitted = round(value);
		let wide_precision = ((precision - scale).max(needed) + wide_scale).min(unscaled::MAX_DIGITS);
		let mut widened = DecimalBuilder::with_capacity(
			Precision::new(wide_precision),
			Scale::new(wide_scale),
			rounded.len() + 1,
		);
		for (index, decimal) in rounded.into_iter().enumerate() {
			match nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)) {
				true => widened.append_null(),
				false => widened.append_fitting(decimal.unscaled()),
			}
		}
		*self = widened;
		fitted
	}
}

fn decimal_nulls<'a>(array: impl Into<DecimalView<'a>>) -> Option<NullBuffer> {
	array.into().array().logical_nulls()
}

#[derive(Debug)]
pub struct WideBuilder<T: WideInt> {
	builder: FixedSizeBinaryBuilder,
	marker: PhantomData<T>,
}

impl<T: WideInt> WideBuilder<T> {
	pub(crate) fn with_capacity(capacity: usize) -> Self {
		Self {
			builder: FixedSizeBinaryBuilder::with_capacity(capacity, T::WIDTH as i32),
			marker: PhantomData,
		}
	}

	pub(crate) fn from_array(array: &FixedSizeBinaryArray) -> Self {
		Self {
			builder: fixed_size_builder(array),
			marker: PhantomData,
		}
	}

	pub(crate) fn append_null(&mut self) {
		let mut row = [0u8; 16];
		T::default().to_ordered(&mut row);
		let null_row = FixedSizeBinaryArray::new(
			T::WIDTH as i32,
			Buffer::from(row[..T::WIDTH].to_vec()),
			Some(NullBuffer::new_null(1)),
		);
		append_fixed_array(&mut self.builder, &null_row)
			.expect("a null row of the builder width always appends");
	}

	pub(crate) fn append_value(&mut self, value: T) {
		let mut row = [0u8; 16];
		value.to_ordered(&mut row);
		append_fixed(&mut self.builder, &row);
	}

	pub(crate) fn append_array(&mut self, array: &FixedSizeBinaryArray) -> Result<()> {
		append_fixed_array(&mut self.builder, array)
	}

	pub(crate) fn len(&self) -> usize {
		self.builder.len()
	}

	pub(crate) fn finish(&mut self) -> FixedSizeBinaryArray {
		self.builder.finish()
	}
}

#[derive(Debug)]
pub struct ColumnBuilder {
	pub(crate) inner: TypedBuilder,
	pub(crate) optional: bool,
}

#[derive(Debug)]
pub enum TypedBuilder {
	Bool(BooleanBuilder),
	Int1(PrimitiveBuilder<Int8Type>),
	Int2(PrimitiveBuilder<Int16Type>),
	Int4(PrimitiveBuilder<Int32Type>),
	Int8(PrimitiveBuilder<Int64Type>),
	Int16(WideBuilder<i128>),
	Uint1(PrimitiveBuilder<UInt8Type>),
	Uint2(PrimitiveBuilder<UInt16Type>),
	Uint4(PrimitiveBuilder<UInt32Type>),
	Uint8(PrimitiveBuilder<UInt64Type>),
	Uint16(WideBuilder<u128>),
	Float4(PrimitiveBuilder<Float32Type>),
	Float8(PrimitiveBuilder<Float64Type>),
	Date(PrimitiveBuilder<Date32Type>),
	DateTime(PrimitiveBuilder<TimestampNanosecondType>),
	Time(PrimitiveBuilder<Time64NanosecondType>),
	Duration(PrimitiveBuilder<IntervalMonthDayNanoType>),
	IdentityId(FixedSizeBinaryBuilder),
	Uuid4(FixedSizeBinaryBuilder),
	Uuid7(FixedSizeBinaryBuilder),
	DictionaryId {
		builder: FixedSizeBinaryBuilder,
		dictionary_id: Option<DictionaryId>,
	},
	Utf8 {
		builder: LargeStringBuilder,
		max_bytes: MaxBytes,
	},
	Blob {
		builder: LargeBinaryBuilder,
		max_bytes: MaxBytes,
	},
	Decimal(DecimalBuilder),
	Any {
		builder: LargeBinaryBuilder,
		declared_type: Option<ValueType>,
	},
	Digest {
		builder: LargeBinaryBuilder,
		inner: ValueType,
		accuracy: u32,
	},
	None(NullBuilder),
}

impl ColumnBuilder {
	pub fn with_capacity(target: ValueType, capacity: usize) -> Self {
		let inner = match target {
			ValueType::Option(inner) => {
				return ColumnBuilder {
					optional: true,
					..ColumnBuilder::with_capacity(*inner, capacity)
				};
			}
			ValueType::Boolean => TypedBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
			ValueType::Int1 => TypedBuilder::Int1(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int2 => TypedBuilder::Int2(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int4 => TypedBuilder::Int4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int8 => TypedBuilder::Int8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int16 => TypedBuilder::Int16(WideBuilder::with_capacity(capacity)),
			ValueType::Uint1 => TypedBuilder::Uint1(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint2 => TypedBuilder::Uint2(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint4 => TypedBuilder::Uint4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint8 => TypedBuilder::Uint8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint16 => TypedBuilder::Uint16(WideBuilder::with_capacity(capacity)),
			ValueType::Float4 => TypedBuilder::Float4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Float8 => TypedBuilder::Float8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Date => TypedBuilder::Date(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::DateTime => TypedBuilder::DateTime(
				PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(capacity)
					.with_timezone(datetime_timezone()),
			),
			ValueType::Time => TypedBuilder::Time(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Duration => TypedBuilder::Duration(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::IdentityId => TypedBuilder::IdentityId(FixedSizeBinaryBuilder::with_capacity(
				capacity,
				UUID_WIDTH as i32,
			)),
			ValueType::Uuid4 => {
				TypedBuilder::Uuid4(FixedSizeBinaryBuilder::with_capacity(capacity, UUID_WIDTH as i32))
			}
			ValueType::Uuid7 => {
				TypedBuilder::Uuid7(FixedSizeBinaryBuilder::with_capacity(capacity, UUID_WIDTH as i32))
			}
			ValueType::DictionaryId => TypedBuilder::DictionaryId {
				builder: FixedSizeBinaryBuilder::with_capacity(capacity, DICTIONARY_ENTRY_WIDTH as i32),
				dictionary_id: None,
			},
			ValueType::Utf8 => TypedBuilder::Utf8 {
				builder: LargeStringBuilder::with_capacity(capacity, capacity * 16),
				max_bytes: MaxBytes::MAX,
			},
			ValueType::Blob => TypedBuilder::Blob {
				builder: LargeBinaryBuilder::with_capacity(capacity, capacity * 32),
				max_bytes: MaxBytes::MAX,
			},
			ValueType::Decimal {
				precision,
				scale,
			} => TypedBuilder::Decimal(DecimalBuilder::with_capacity(precision, scale, capacity)),
			ValueType::Any | ValueType::Tuple(_) => TypedBuilder::Any {
				builder: LargeBinaryBuilder::with_capacity(capacity, 0),
				declared_type: None,
			},
			declared @ (ValueType::List(_) | ValueType::Record(_)) => TypedBuilder::Any {
				builder: LargeBinaryBuilder::with_capacity(capacity, 0),
				declared_type: Some(declared),
			},
			ValueType::Digest {
				inner,
				accuracy,
			} => TypedBuilder::Digest {
				builder: LargeBinaryBuilder::with_capacity(capacity, 0),
				inner: *inner,
				accuracy,
			},
		};
		ColumnBuilder {
			inner,
			optional: false,
		}
	}

	pub fn like(view: &ColumnView, capacity: usize) -> Self {
		if view.is_none() {
			return ColumnBuilder::untyped_none();
		}
		let mut builder = ColumnBuilder::with_capacity(view.get_type(), capacity);
		builder.copy_details(&view.data);
		builder
	}

	pub fn from_view(view: &ColumnView) -> Self {
		ColumnBuilder {
			inner: TypedBuilder::from_view(&view.data),
			optional: view.is_nullable() || view.none_count() > 0,
		}
	}

	pub(crate) fn untyped_none() -> Self {
		ColumnBuilder {
			inner: TypedBuilder::None(NullBuilder::new()),
			optional: true,
		}
	}

	pub fn inner(&self) -> &TypedBuilder {
		&self.inner
	}

	pub fn push<T>(&mut self, value: T)
	where
		ColumnBuilder: Push<T>,
		T: Debug,
	{
		<Self as Push<T>>::push(self, value)
	}

	pub fn set_dictionary_id(&mut self, id: DictionaryId) {
		if let TypedBuilder::DictionaryId {
			dictionary_id,
			..
		} = &mut self.inner
		{
			*dictionary_id = Some(id);
		}
	}

	pub fn dictionary_id(&self) -> Option<DictionaryId> {
		match &self.inner {
			TypedBuilder::DictionaryId {
				dictionary_id,
				..
			} => *dictionary_id,
			_ => None,
		}
	}

	pub fn extend(&mut self, other: &ColumnView) -> Result<()> {
		match (self.is_untyped_none(), other.is_untyped_none()) {
			(true, true) => {
				let len = self.len() + other.len();
				*self = ColumnBuilder::untyped_none();
				self.append_nones(len);
				return Ok(());
			}
			(true, false) => {
				let len = self.len();
				*self = ColumnBuilder::like(other, len + other.len());
				self.append_nones(len);
				self.optional = true;
			}
			(false, true) => {
				self.append_nones(other.len());
				return Ok(());
			}
			(false, false) => {}
		}
		let other_optional = other.is_nullable() || other.none_count() > 0;
		if self.optional && other_optional && !self.same_type(other) {
			return internal_err!("column type mismatch in Option extend");
		}
		self.append_view(other)?;
		self.optional |= other_optional;
		Ok(())
	}

	pub fn append_values(&mut self, other: &ColumnView) -> Result<()> {
		let nones = other.none_count();
		if nones == other.len() {
			self.append_nones(other.len());
			return Ok(());
		}
		self.append_view(other)?;
		self.optional |= nones > 0;
		Ok(())
	}

	fn append_nones(&mut self, count: usize) {
		for _ in 0..count {
			self.push_none();
		}
	}

	fn same_type(&self, other: &ColumnView) -> bool {
		let same_family =
			matches!((&self.inner, &other.data), (TypedBuilder::Decimal(_), ViewData::Decimal(_)));
		same_family || self.inner.get_type() == other.base_type()
	}

	fn is_untyped_none(&self) -> bool {
		match &self.inner {
			TypedBuilder::None(_) => true,
			TypedBuilder::Any {
				builder,
				declared_type: None,
			} if self.optional => match builder.validity_slice() {
				Some(bits) => (0..builder.len()).all(|index| !bit_util::get_bit(bits, index)),
				None => builder.is_empty(),
			},
			_ => false,
		}
	}

	fn copy_details(&mut self, data: &ViewData) {
		match (data, &mut self.inner) {
			(
				ViewData::Utf8 {
					max_bytes: source,
					..
				},
				TypedBuilder::Utf8 {
					max_bytes: target,
					..
				},
			)
			| (
				ViewData::Blob {
					max_bytes: source,
					..
				},
				TypedBuilder::Blob {
					max_bytes: target,
					..
				},
			) => *target = *source,
			(
				ViewData::DictionaryId {
					dictionary_id: Some(source),
					..
				},
				TypedBuilder::DictionaryId {
					dictionary_id: target,
					..
				},
			) => *target = Some(*source),
			_ => {}
		}
	}

	fn append_view(&mut self, other: &ColumnView) -> Result<()> {
		match (&mut self.inner, &other.data) {
			(TypedBuilder::Bool(l), ViewData::Bool(r)) => l.append_array(r),
			(TypedBuilder::Int1(l), ViewData::Int1(r)) => l.append_array(r),
			(TypedBuilder::Int2(l), ViewData::Int2(r)) => l.append_array(r),
			(TypedBuilder::Int4(l), ViewData::Int4(r)) => l.append_array(r),
			(TypedBuilder::Int8(l), ViewData::Int8(r)) => l.append_array(r),
			(TypedBuilder::Int16(l), ViewData::Int16(r)) => l.append_array(r)?,
			(TypedBuilder::Uint1(l), ViewData::Uint1(r)) => l.append_array(r),
			(TypedBuilder::Uint2(l), ViewData::Uint2(r)) => l.append_array(r),
			(TypedBuilder::Uint4(l), ViewData::Uint4(r)) => l.append_array(r),
			(TypedBuilder::Uint8(l), ViewData::Uint8(r)) => l.append_array(r),
			(TypedBuilder::Uint16(l), ViewData::Uint16(r)) => l.append_array(r)?,
			(TypedBuilder::Float4(l), ViewData::Float4(r)) => l.append_array(r),
			(TypedBuilder::Float8(l), ViewData::Float8(r)) => l.append_array(r),
			(TypedBuilder::Date(l), ViewData::Date(r)) => l.append_array(r),
			(TypedBuilder::DateTime(l), ViewData::DateTime(r)) => l.append_array(r),
			(TypedBuilder::Time(l), ViewData::Time(r)) => l.append_array(r),
			(TypedBuilder::Duration(l), ViewData::Duration(r)) => l.append_array(r),
			(TypedBuilder::IdentityId(l), ViewData::IdentityId(r)) => append_fixed_array(l, r)?,
			(TypedBuilder::Uuid4(l), ViewData::Uuid4(r)) => append_fixed_array(l, r)?,
			(TypedBuilder::Uuid7(l), ViewData::Uuid7(r)) => append_fixed_array(l, r)?,
			(
				TypedBuilder::DictionaryId {
					builder,
					..
				},
				ViewData::DictionaryId {
					container,
					..
				},
			) => append_fixed_array(builder, container)?,
			(
				TypedBuilder::Utf8 {
					builder,
					..
				},
				ViewData::Utf8 {
					container,
					..
				},
			) => append_varlen(builder, container)?,
			(
				TypedBuilder::Blob {
					builder,
					..
				},
				ViewData::Blob {
					container,
					..
				},
			) => append_varlen(builder, container)?,
			(TypedBuilder::Decimal(l), ViewData::Decimal(r)) => l.append_array(*r),
			(
				TypedBuilder::Any {
					builder,
					..
				},
				ViewData::Any {
					container,
					..
				},
			) => append_varlen(builder, container)?,
			(
				TypedBuilder::Digest {
					builder,
					inner: l_inner,
					accuracy: l_accuracy,
				},
				ViewData::Digest {
					container,
					inner: r_inner,
					accuracy: r_accuracy,
				},
			) if l_inner == r_inner && l_accuracy == r_accuracy => append_varlen(builder, container)?,
			_ => return internal_err!("column type mismatch"),
		}
		Ok(())
	}

	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn get_type(&self) -> ValueType {
		match self.optional {
			true => ValueType::Option(Box::new(self.inner.get_type())),
			false => self.inner.get_type(),
		}
	}

	pub fn finish(self, name: &str) -> (FieldRef, ArrayRef) {
		let (mut field_type, array) = self.inner.finish();
		if let Some(value_type) = field_type.value_type.take() {
			field_type.value_type = Some(match self.optional || array.null_count() > 0 {
				true => ValueType::Option(Box::new(value_type)),
				false => value_type,
			});
		}
		named(name, field_type, array)
	}
}

impl TypedBuilder {
	pub(crate) fn append_null(&mut self) {
		match self {
			TypedBuilder::Bool(b) => b.append_null(),
			TypedBuilder::Int1(b) => b.append_null(),
			TypedBuilder::Int2(b) => b.append_null(),
			TypedBuilder::Int4(b) => b.append_null(),
			TypedBuilder::Int8(b) => b.append_null(),
			TypedBuilder::Int16(b) => b.append_null(),
			TypedBuilder::Uint1(b) => b.append_null(),
			TypedBuilder::Uint2(b) => b.append_null(),
			TypedBuilder::Uint4(b) => b.append_null(),
			TypedBuilder::Uint8(b) => b.append_null(),
			TypedBuilder::Uint16(b) => b.append_null(),
			TypedBuilder::Float4(b) => b.append_null(),
			TypedBuilder::Float8(b) => b.append_null(),
			TypedBuilder::Date(b) => b.append_null(),
			TypedBuilder::DateTime(b) => b.append_null(),
			TypedBuilder::Time(b) => b.append_null(),
			TypedBuilder::Duration(b) => b.append_null(),
			TypedBuilder::IdentityId(b) => b.append_null(),
			TypedBuilder::Uuid4(b) => b.append_null(),
			TypedBuilder::Uuid7(b) => b.append_null(),
			TypedBuilder::DictionaryId {
				builder,
				..
			} => builder.append_null(),
			TypedBuilder::Utf8 {
				builder,
				..
			} => builder.append_null(),
			TypedBuilder::Blob {
				builder,
				..
			}
			| TypedBuilder::Any {
				builder,
				..
			}
			| TypedBuilder::Digest {
				builder,
				..
			} => builder.append_null(),
			TypedBuilder::Decimal(b) => b.append_null(),
			TypedBuilder::None(b) => b.append_null(),
		}
	}

	pub fn len(&self) -> usize {
		match self {
			TypedBuilder::Bool(b) => b.len(),
			TypedBuilder::Int1(b) => b.len(),
			TypedBuilder::Int2(b) => b.len(),
			TypedBuilder::Int4(b) => b.len(),
			TypedBuilder::Int8(b) => b.len(),
			TypedBuilder::Int16(b) => b.len(),
			TypedBuilder::Uint1(b) => b.len(),
			TypedBuilder::Uint2(b) => b.len(),
			TypedBuilder::Uint4(b) => b.len(),
			TypedBuilder::Uint8(b) => b.len(),
			TypedBuilder::Uint16(b) => b.len(),
			TypedBuilder::Float4(b) => b.len(),
			TypedBuilder::Float8(b) => b.len(),
			TypedBuilder::Date(b) => b.len(),
			TypedBuilder::DateTime(b) => b.len(),
			TypedBuilder::Time(b) => b.len(),
			TypedBuilder::Duration(b) => b.len(),
			TypedBuilder::IdentityId(b) => b.len(),
			TypedBuilder::Uuid4(b) => b.len(),
			TypedBuilder::Uuid7(b) => b.len(),
			TypedBuilder::DictionaryId {
				builder,
				..
			} => builder.len(),
			TypedBuilder::Utf8 {
				builder,
				..
			} => builder.len(),
			TypedBuilder::Blob {
				builder,
				..
			}
			| TypedBuilder::Any {
				builder,
				..
			}
			| TypedBuilder::Digest {
				builder,
				..
			} => builder.len(),
			TypedBuilder::Decimal(b) => b.len(),
			TypedBuilder::None(b) => b.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn get_type(&self) -> ValueType {
		match self {
			TypedBuilder::Bool(_) => ValueType::Boolean,
			TypedBuilder::Int1(_) => ValueType::Int1,
			TypedBuilder::Int2(_) => ValueType::Int2,
			TypedBuilder::Int4(_) => ValueType::Int4,
			TypedBuilder::Int8(_) => ValueType::Int8,
			TypedBuilder::Int16(_) => ValueType::Int16,
			TypedBuilder::Uint1(_) => ValueType::Uint1,
			TypedBuilder::Uint2(_) => ValueType::Uint2,
			TypedBuilder::Uint4(_) => ValueType::Uint4,
			TypedBuilder::Uint8(_) => ValueType::Uint8,
			TypedBuilder::Uint16(_) => ValueType::Uint16,
			TypedBuilder::Float4(_) => ValueType::Float4,
			TypedBuilder::Float8(_) => ValueType::Float8,
			TypedBuilder::Date(_) => ValueType::Date,
			TypedBuilder::DateTime(_) => ValueType::DateTime,
			TypedBuilder::Time(_) => ValueType::Time,
			TypedBuilder::Duration(_) => ValueType::Duration,
			TypedBuilder::IdentityId(_) => ValueType::IdentityId,
			TypedBuilder::Uuid4(_) => ValueType::Uuid4,
			TypedBuilder::Uuid7(_) => ValueType::Uuid7,
			TypedBuilder::DictionaryId {
				..
			} => ValueType::DictionaryId,
			TypedBuilder::Utf8 {
				..
			} => ValueType::Utf8,
			TypedBuilder::Blob {
				..
			} => ValueType::Blob,
			TypedBuilder::Decimal(b) => ValueType::decimal(b.precision(), b.scale()),
			TypedBuilder::Any {
				declared_type,
				..
			} => declared_type.clone().unwrap_or(ValueType::Any),
			TypedBuilder::Digest {
				inner,
				accuracy,
				..
			} => ValueType::Digest {
				inner: Box::new(inner.clone()),
				accuracy: *accuracy,
			},
			TypedBuilder::None(_) => ValueType::Any,
		}
	}

	fn finish(self) -> (FieldType, ArrayRef) {
		let value_type = self.get_type();
		let bare = |array: ArrayRef| {
			(
				FieldType {
					value_type: Some(value_type.clone()),
					..FieldType::default()
				},
				array,
			)
		};
		match self {
			TypedBuilder::Bool(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Int1(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Int2(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Int4(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Int8(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Int16(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uint1(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uint2(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uint4(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uint8(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uint16(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Float4(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Float8(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Date(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::DateTime(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Time(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Duration(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::IdentityId(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uuid4(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::Uuid7(mut b) => bare(Arc::new(b.finish())),
			TypedBuilder::DictionaryId {
				mut builder,
				dictionary_id,
			} => (
				FieldType {
					value_type: Some(value_type),
					dictionary_id,
					..FieldType::default()
				},
				Arc::new(builder.finish()),
			),
			TypedBuilder::Utf8 {
				mut builder,
				max_bytes,
			} => (
				FieldType {
					value_type: Some(value_type),
					max_bytes: limit(max_bytes),
					..FieldType::default()
				},
				Arc::new(builder.finish()),
			),
			TypedBuilder::Blob {
				mut builder,
				max_bytes,
			} => (
				FieldType {
					value_type: Some(value_type),
					max_bytes: limit(max_bytes),
					..FieldType::default()
				},
				Arc::new(builder.finish()),
			),
			TypedBuilder::Decimal(mut b) => bare(b.finish().into_array()),
			TypedBuilder::Any {
				mut builder,
				declared_type,
			} => (
				FieldType {
					value_type: Some(value_type),
					declared_type,
					..FieldType::default()
				},
				Arc::new(builder.finish()),
			),
			TypedBuilder::Digest {
				mut builder,
				..
			} => bare(Arc::new(builder.finish())),
			TypedBuilder::None(mut b) => (FieldType::default(), Arc::new(b.finish())),
		}
	}

	fn from_view(data: &ViewData) -> TypedBuilder {
		match data {
			ViewData::Bool(a) => {
				let mut builder = BooleanBuilder::with_capacity(a.len());
				builder.append_array(a);
				TypedBuilder::Bool(builder)
			}
			ViewData::Int1(a) => TypedBuilder::Int1(primitive_builder((*a).clone())),
			ViewData::Int2(a) => TypedBuilder::Int2(primitive_builder((*a).clone())),
			ViewData::Int4(a) => TypedBuilder::Int4(primitive_builder((*a).clone())),
			ViewData::Int8(a) => TypedBuilder::Int8(primitive_builder((*a).clone())),
			ViewData::Int16(a) => TypedBuilder::Int16(WideBuilder::from_array(a)),
			ViewData::Uint1(a) => TypedBuilder::Uint1(primitive_builder((*a).clone())),
			ViewData::Uint2(a) => TypedBuilder::Uint2(primitive_builder((*a).clone())),
			ViewData::Uint4(a) => TypedBuilder::Uint4(primitive_builder((*a).clone())),
			ViewData::Uint8(a) => TypedBuilder::Uint8(primitive_builder((*a).clone())),
			ViewData::Uint16(a) => TypedBuilder::Uint16(WideBuilder::from_array(a)),
			ViewData::Float4(a) => TypedBuilder::Float4(primitive_builder((*a).clone())),
			ViewData::Float8(a) => TypedBuilder::Float8(primitive_builder((*a).clone())),
			ViewData::Date(a) => TypedBuilder::Date(primitive_builder((*a).clone())),
			ViewData::DateTime(a) => TypedBuilder::DateTime(primitive_builder((*a).clone())),
			ViewData::Time(a) => TypedBuilder::Time(primitive_builder((*a).clone())),
			ViewData::Duration(a) => TypedBuilder::Duration(primitive_builder((*a).clone())),
			ViewData::IdentityId(a) => TypedBuilder::IdentityId(fixed_size_builder(a)),
			ViewData::Uuid4(a) => TypedBuilder::Uuid4(fixed_size_builder(a)),
			ViewData::Uuid7(a) => TypedBuilder::Uuid7(fixed_size_builder(a)),
			ViewData::DictionaryId {
				container,
				dictionary_id,
			} => TypedBuilder::DictionaryId {
				builder: fixed_size_builder(container),
				dictionary_id: *dictionary_id,
			},
			ViewData::Utf8 {
				container,
				max_bytes,
			} => TypedBuilder::Utf8 {
				builder: varlen_builder((*container).clone()),
				max_bytes: *max_bytes,
			},
			ViewData::Blob {
				container,
				max_bytes,
			} => TypedBuilder::Blob {
				builder: varlen_builder((*container).clone()),
				max_bytes: *max_bytes,
			},
			ViewData::Decimal(a) => {
				TypedBuilder::Decimal(DecimalBuilder::from_array(DecimalArray::from(*a)))
			}
			ViewData::Any {
				container,
				declared_type,
			} => TypedBuilder::Any {
				builder: varlen_builder((*container).clone()),
				declared_type: declared_type.clone(),
			},
			ViewData::Digest {
				container,
				inner,
				accuracy,
			} => TypedBuilder::Digest {
				builder: varlen_builder((*container).clone()),
				inner: inner.clone(),
				accuracy: *accuracy,
			},
			ViewData::None {
				array,
			} => {
				let mut builder = NullBuilder::new();
				builder.append_nulls(array.len());
				TypedBuilder::None(builder)
			}
		}
	}
}

fn limit(max_bytes: MaxBytes) -> Option<MaxBytes> {
	(max_bytes != MaxBytes::MAX).then_some(max_bytes)
}

pub(crate) fn primitive_builder<A>(array: PrimitiveArray<A>) -> PrimitiveBuilder<A>
where
	A: ArrowPrimitiveType,
{
	let data_type = array.data_type().clone();
	match array.into_builder() {
		Ok(builder) => builder.with_data_type(data_type),
		Err(array) => {
			let mut builder = PrimitiveBuilder::with_capacity(array.len());
			builder.append_array(&array);
			builder.with_data_type(data_type)
		}
	}
}

pub(crate) fn append_fixed(builder: &mut FixedSizeBinaryBuilder, row: &[u8]) {
	if builder.append_value(row).is_err() {
		panic!(
			"a fixed size builder of {} byte rows can not take a row of {} bytes",
			builder.finish_cloned().value_length(),
			row.len()
		);
	}
}

fn fixed_size_builder(array: &FixedSizeBinaryArray) -> FixedSizeBinaryBuilder {
	let mut builder = FixedSizeBinaryBuilder::with_capacity(array.len(), array.value_length());
	append_fixed_array(&mut builder, array)
		.expect("copying a fixed size array into a builder of its own width failed");
	builder
}

pub(crate) fn append_fixed_array(builder: &mut FixedSizeBinaryBuilder, array: &FixedSizeBinaryArray) -> Result<()> {
	builder.append_array(array).map_err(|e| internal_error!("fixed size append failed: {}", e))
}

pub(crate) fn varlen_builder<T>(array: GenericByteArray<T>) -> GenericByteBuilder<T>
where
	T: ByteArrayType<Offset = i64>,
{
	let array = if array.value_offsets()[0] == 0 {
		match array.into_builder() {
			Ok(builder) => return builder,
			Err(array) => array,
		}
	} else {
		array
	};
	let (data, _) = varlen_array::compact_parts(&array);
	let mut builder = GenericByteBuilder::<T>::with_capacity(array.len(), data.len());
	append_varlen(&mut builder, &array)
		.expect("copying a shared or offset utf8 / blob array into a new builder failed");
	builder
}

pub(crate) fn append_varlen<T>(builder: &mut GenericByteBuilder<T>, array: &GenericByteArray<T>) -> Result<()>
where
	T: ByteArrayType<Offset = i64>,
{
	builder.append_array(array).map_err(|e| internal_error!("utf8 / blob append failed: {}", e))
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use arrow_array::ArrayRef;
	use arrow_buffer::{BooleanBuffer, NullBuffer};
	use arrow_schema::FieldRef;
	use reifydb_value::value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		value_type::ValueType,
	};

	use super::*;
	use crate::value::column::{factory, nulls::with_nulls};

	fn concat(parts: &[(FieldRef, ArrayRef)]) -> (FieldRef, ArrayRef) {
		let (first, rest) = parts.split_first().unwrap();
		let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(first).unwrap());
		for part in rest {
			builder.extend(&ColumnView::try_from(part).unwrap()).unwrap();
		}
		builder.finish("c")
	}

	#[test]
	fn concat_keeps_a_nullable_column_nullable_across_chunks() {
		// An all-valid chunk still declares Option, and joining it with a bare chunk must not drop that.
		let first = with_nulls(factory::int4("c", [1, 2]), NullBuffer::new_valid(2)).unwrap();
		let second = factory::int4("c", [3, 4]);

		let joined = concat(&[first, second]);
		let joined = ColumnView::try_from(&joined).unwrap();

		assert_eq!(joined.len(), 4);
		assert!(joined.is_nullable());
		assert_eq!(joined.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		assert_eq!(joined.get_value(3), Value::Int4(4));
	}

	#[test]
	fn concat_carries_the_none_rows_of_every_chunk() {
		// The null bits of each chunk have to land at that chunk's offset, never be rebuilt as all valid.
		let first = factory::int4_with_bitvec("c", [1, 0], BooleanBuffer::from(vec![true, false]));
		let second = factory::int4_with_bitvec("c", [0, 4], BooleanBuffer::from(vec![false, true]));

		let joined = concat(&[first, second]);
		let joined = ColumnView::try_from(&joined).unwrap();

		assert_eq!(joined.get_value(0), Value::Int4(1));
		assert_eq!(joined.get_value(1), Value::none_of(ValueType::Int4));
		assert_eq!(joined.get_value(2), Value::none_of(ValueType::Int4));
		assert_eq!(joined.get_value(3), Value::Int4(4));
	}

	#[test]
	fn concat_keeps_the_decimal_precision_and_scale() {
		// Precision and scale live in the arrow type, so joining chunks must never reset them to defaults.
		let first =
			factory::decimal("c", Precision::new(9), Scale::new(2), [Decimal::from_str("1.25").unwrap()]);
		let second =
			factory::decimal("c", Precision::new(9), Scale::new(2), [Decimal::from_str("2.50").unwrap()]);

		let joined = concat(&[first, second]);
		let joined = ColumnView::try_from(&joined).unwrap();

		let ViewData::Decimal(array) = &joined.data else {
			panic!("expected a decimal column");
		};
		assert_eq!(array.precision(), Precision::new(9));
		assert_eq!(array.scale(), Scale::new(2));
		assert_eq!(joined.len(), 2);
	}
}
