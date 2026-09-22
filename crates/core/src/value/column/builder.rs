// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, mem};

use arrow_array::{
	Array, ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray, GenericByteArray, PrimitiveArray,
	builder::{ArrayBuilder, GenericByteBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder},
	types::{
		ByteArrayType, Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
		Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType, Time64NanosecondType, UInt8Type, UInt16Type,
		UInt32Type, UInt64Type,
	},
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer};
use reifydb_value::{
	Result,
	value::{
		Value,
		constraint::bytes::MaxBytes,
		container::{
			decimal_array::{INT16_DATA_TYPE, UINT16_DATA_TYPE, with_int16_type, with_uint16_type},
			dictionary_array::{self, DICTIONARY_ENTRY_WIDTH},
			uuid_array::{self, UUID_WIDTH},
			varlen_array,
		},
		dictionary::DictionaryId,
		value_type::ValueType,
	},
};

use crate::{
	internal_error,
	value::column::{
		buffer::{ColumnBuffer, with_container},
		push::Push,
	},
};

#[derive(Debug)]
pub enum ColumnBuilder {
	Bool(BooleanBufferBuilder),
	Int1(PrimitiveBuilder<Int8Type>),
	Int2(PrimitiveBuilder<Int16Type>),
	Int4(PrimitiveBuilder<Int32Type>),
	Int8(PrimitiveBuilder<Int64Type>),
	Int16(PrimitiveBuilder<Decimal128Type>),
	Uint1(PrimitiveBuilder<UInt8Type>),
	Uint2(PrimitiveBuilder<UInt16Type>),
	Uint4(PrimitiveBuilder<UInt32Type>),
	Uint8(PrimitiveBuilder<UInt64Type>),
	Uint16(PrimitiveBuilder<Decimal256Type>),
	Float4(PrimitiveBuilder<Float32Type>),
	Float8(PrimitiveBuilder<Float64Type>),
	Date(PrimitiveBuilder<Date32Type>),
	DateTime(PrimitiveBuilder<UInt64Type>),
	Time(PrimitiveBuilder<Time64NanosecondType>),
	Duration(PrimitiveBuilder<IntervalMonthDayNanoType>),
	IdentityId(MutableBuffer),
	Uuid4(MutableBuffer),
	Uuid7(MutableBuffer),
	DictionaryId {
		buffer: MutableBuffer,
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
	Option {
		inner: Box<ColumnBuilder>,
		bitvec: BooleanBufferBuilder,
	},
	Buffer(ColumnBuffer),
}

impl ColumnBuilder {
	pub fn with_capacity(target: ValueType, capacity: usize) -> Self {
		match target {
			ValueType::Boolean => ColumnBuilder::Bool(BooleanBufferBuilder::new(capacity)),
			ValueType::Int1 => ColumnBuilder::Int1(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int2 => ColumnBuilder::Int2(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int4 => ColumnBuilder::Int4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int8 => ColumnBuilder::Int8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Int16 => ColumnBuilder::Int16(
				PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity)
					.with_data_type(INT16_DATA_TYPE),
			),
			ValueType::Uint1 => ColumnBuilder::Uint1(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint2 => ColumnBuilder::Uint2(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint4 => ColumnBuilder::Uint4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint8 => ColumnBuilder::Uint8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Uint16 => ColumnBuilder::Uint16(
				PrimitiveBuilder::<Decimal256Type>::with_capacity(capacity)
					.with_data_type(UINT16_DATA_TYPE),
			),
			ValueType::Float4 => ColumnBuilder::Float4(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Float8 => ColumnBuilder::Float8(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Date => ColumnBuilder::Date(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::DateTime => ColumnBuilder::DateTime(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Time => ColumnBuilder::Time(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::Duration => ColumnBuilder::Duration(PrimitiveBuilder::with_capacity(capacity)),
			ValueType::IdentityId => {
				ColumnBuilder::IdentityId(MutableBuffer::with_capacity(capacity * UUID_WIDTH))
			}
			ValueType::Uuid4 => ColumnBuilder::Uuid4(MutableBuffer::with_capacity(capacity * UUID_WIDTH)),
			ValueType::Uuid7 => ColumnBuilder::Uuid7(MutableBuffer::with_capacity(capacity * UUID_WIDTH)),
			ValueType::DictionaryId => ColumnBuilder::DictionaryId {
				buffer: MutableBuffer::with_capacity(capacity * DICTIONARY_ENTRY_WIDTH),
				dictionary_id: None,
			},
			ValueType::Utf8 => ColumnBuilder::Utf8 {
				builder: LargeStringBuilder::with_capacity(capacity, capacity * 16),
				max_bytes: MaxBytes::MAX,
			},
			ValueType::Blob => ColumnBuilder::Blob {
				builder: LargeBinaryBuilder::with_capacity(capacity, capacity * 32),
				max_bytes: MaxBytes::MAX,
			},
			ValueType::Option(inner) => ColumnBuilder::Option {
				inner: Box::new(ColumnBuilder::with_capacity(*inner, capacity)),
				bitvec: BooleanBufferBuilder::new(capacity),
			},
			other => ColumnBuilder::Buffer(ColumnBuffer::with_capacity(other, capacity)),
		}
	}

	pub fn like(buffer: &ColumnBuffer, capacity: usize) -> Self {
		buffer.empty_like(capacity).into_builder()
	}

	pub fn push<T>(&mut self, value: T)
	where
		ColumnBuilder: Push<T>,
		T: Debug,
	{
		<Self as Push<T>>::push(self, value)
	}

	pub(crate) fn push_keeping_option(&mut self, value: Value) {
		match self {
			ColumnBuilder::Option {
				inner,
				bitvec,
			} if !matches!(value, Value::None { .. }) => {
				inner.push_value(value);
				bitvec.append(true);
			}
			builder => builder.push_value(value),
		}
	}

	pub(crate) fn push_default(&mut self) {
		match self {
			ColumnBuilder::Bool(b) => b.append(false),
			ColumnBuilder::Int1(b) => b.append_value(Default::default()),
			ColumnBuilder::Int2(b) => b.append_value(Default::default()),
			ColumnBuilder::Int4(b) => b.append_value(Default::default()),
			ColumnBuilder::Int8(b) => b.append_value(Default::default()),
			ColumnBuilder::Int16(b) => b.append_value(Default::default()),
			ColumnBuilder::Uint1(b) => b.append_value(Default::default()),
			ColumnBuilder::Uint2(b) => b.append_value(Default::default()),
			ColumnBuilder::Uint4(b) => b.append_value(Default::default()),
			ColumnBuilder::Uint8(b) => b.append_value(Default::default()),
			ColumnBuilder::Uint16(b) => b.append_value(Default::default()),
			ColumnBuilder::Float4(b) => b.append_value(Default::default()),
			ColumnBuilder::Float8(b) => b.append_value(Default::default()),
			ColumnBuilder::Date(b) => b.append_value(Default::default()),
			ColumnBuilder::DateTime(b) => b.append_value(Default::default()),
			ColumnBuilder::Time(b) => b.append_value(Default::default()),
			ColumnBuilder::Duration(b) => b.append_value(Default::default()),
			ColumnBuilder::IdentityId(b) => b.extend_zeros(UUID_WIDTH),
			ColumnBuilder::Uuid4(b) => b.extend_zeros(UUID_WIDTH),
			ColumnBuilder::Uuid7(b) => b.extend_zeros(UUID_WIDTH),
			ColumnBuilder::DictionaryId {
				buffer,
				..
			} => buffer.extend_zeros(DICTIONARY_ENTRY_WIDTH),
			ColumnBuilder::Utf8 {
				builder,
				..
			} => builder.append_value(""),
			ColumnBuilder::Blob {
				builder,
				..
			} => builder.append_value(b""),
			ColumnBuilder::Option {
				..
			} => {
				unreachable!(
					"with_container! must not be called on Option variant directly; handle it explicitly"
				)
			}
			ColumnBuilder::Buffer(buffer) => with_container!(buffer, |c| c.push_default()),
		}
	}

	pub fn set_dictionary_id(&mut self, id: DictionaryId) {
		if let ColumnBuilder::DictionaryId {
			dictionary_id,
			..
		} = self
		{
			*dictionary_id = Some(id);
		}
	}

	pub fn extend(&mut self, other: ColumnBuffer) -> Result<()> {
		match (&mut *self, other) {
			(ColumnBuilder::Bool(l), ColumnBuffer::Bool(r)) => l.append_buffer(r.values()),
			(ColumnBuilder::Int1(l), ColumnBuffer::Int1(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Int2(l), ColumnBuffer::Int2(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Int4(l), ColumnBuffer::Int4(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Int8(l), ColumnBuffer::Int8(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Int16(l), ColumnBuffer::Int16(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Uint1(l), ColumnBuffer::Uint1(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Uint2(l), ColumnBuffer::Uint2(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Uint4(l), ColumnBuffer::Uint4(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Uint8(l), ColumnBuffer::Uint8(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Uint16(l), ColumnBuffer::Uint16(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Float4(l), ColumnBuffer::Float4(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Float8(l), ColumnBuffer::Float8(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Date(l), ColumnBuffer::Date(r)) => l.append_slice(r.values()),
			(ColumnBuilder::DateTime(l), ColumnBuffer::DateTime(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Time(l), ColumnBuffer::Time(r)) => l.append_slice(r.values()),
			(ColumnBuilder::Duration(l), ColumnBuffer::Duration(r)) => l.append_slice(r.values()),
			(ColumnBuilder::IdentityId(l), ColumnBuffer::IdentityId(r)) => {
				l.extend_from_slice(r.value_data())
			}
			(ColumnBuilder::Uuid4(l), ColumnBuffer::Uuid4(r)) => l.extend_from_slice(r.value_data()),
			(ColumnBuilder::Uuid7(l), ColumnBuffer::Uuid7(r)) => l.extend_from_slice(r.value_data()),
			(
				ColumnBuilder::DictionaryId {
					buffer,
					..
				},
				ColumnBuffer::DictionaryId {
					container,
					..
				},
			) => buffer.extend_from_slice(container.value_data()),
			(
				ColumnBuilder::Utf8 {
					builder,
					..
				},
				ColumnBuffer::Utf8 {
					container,
					..
				},
			) => append_varlen(builder, &container)?,
			(
				ColumnBuilder::Blob {
					builder,
					..
				},
				ColumnBuffer::Blob {
					container,
					..
				},
			) => append_varlen(builder, &container)?,
			(_, other) => {
				let mut buffer =
					mem::replace(self, ColumnBuilder::Bool(BooleanBufferBuilder::new(0))).finish();
				let extended = buffer.extend(other);
				*self = buffer.into_builder();
				extended?;
			}
		}
		Ok(())
	}

	pub fn len(&self) -> usize {
		match self {
			ColumnBuilder::Bool(b) => b.len(),
			ColumnBuilder::Int1(b) => b.len(),
			ColumnBuilder::Int2(b) => b.len(),
			ColumnBuilder::Int4(b) => b.len(),
			ColumnBuilder::Int8(b) => b.len(),
			ColumnBuilder::Int16(b) => b.len(),
			ColumnBuilder::Uint1(b) => b.len(),
			ColumnBuilder::Uint2(b) => b.len(),
			ColumnBuilder::Uint4(b) => b.len(),
			ColumnBuilder::Uint8(b) => b.len(),
			ColumnBuilder::Uint16(b) => b.len(),
			ColumnBuilder::Float4(b) => b.len(),
			ColumnBuilder::Float8(b) => b.len(),
			ColumnBuilder::Date(b) => b.len(),
			ColumnBuilder::DateTime(b) => b.len(),
			ColumnBuilder::Time(b) => b.len(),
			ColumnBuilder::Duration(b) => b.len(),
			ColumnBuilder::IdentityId(b) => b.len() / UUID_WIDTH,
			ColumnBuilder::Uuid4(b) => b.len() / UUID_WIDTH,
			ColumnBuilder::Uuid7(b) => b.len() / UUID_WIDTH,
			ColumnBuilder::DictionaryId {
				buffer,
				..
			} => buffer.len() / DICTIONARY_ENTRY_WIDTH,
			ColumnBuilder::Utf8 {
				builder,
				..
			} => builder.len(),
			ColumnBuilder::Blob {
				builder,
				..
			} => builder.len(),
			ColumnBuilder::Option {
				inner,
				..
			} => inner.len(),
			ColumnBuilder::Buffer(buffer) => buffer.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn get_type(&self) -> ValueType {
		match self {
			ColumnBuilder::Bool(_) => ValueType::Boolean,
			ColumnBuilder::Int1(_) => ValueType::Int1,
			ColumnBuilder::Int2(_) => ValueType::Int2,
			ColumnBuilder::Int4(_) => ValueType::Int4,
			ColumnBuilder::Int8(_) => ValueType::Int8,
			ColumnBuilder::Int16(_) => ValueType::Int16,
			ColumnBuilder::Uint1(_) => ValueType::Uint1,
			ColumnBuilder::Uint2(_) => ValueType::Uint2,
			ColumnBuilder::Uint4(_) => ValueType::Uint4,
			ColumnBuilder::Uint8(_) => ValueType::Uint8,
			ColumnBuilder::Uint16(_) => ValueType::Uint16,
			ColumnBuilder::Float4(_) => ValueType::Float4,
			ColumnBuilder::Float8(_) => ValueType::Float8,
			ColumnBuilder::Date(_) => ValueType::Date,
			ColumnBuilder::DateTime(_) => ValueType::DateTime,
			ColumnBuilder::Time(_) => ValueType::Time,
			ColumnBuilder::Duration(_) => ValueType::Duration,
			ColumnBuilder::IdentityId(_) => ValueType::IdentityId,
			ColumnBuilder::Uuid4(_) => ValueType::Uuid4,
			ColumnBuilder::Uuid7(_) => ValueType::Uuid7,
			ColumnBuilder::DictionaryId {
				..
			} => ValueType::DictionaryId,
			ColumnBuilder::Utf8 {
				..
			} => ValueType::Utf8,
			ColumnBuilder::Blob {
				..
			} => ValueType::Blob,
			ColumnBuilder::Option {
				inner,
				..
			} => ValueType::Option(Box::new(inner.get_type())),
			ColumnBuilder::Buffer(buffer) => buffer.get_type(),
		}
	}

	pub fn finish(self) -> ColumnBuffer {
		match self {
			ColumnBuilder::Bool(mut b) => ColumnBuffer::Bool(BooleanArray::from(b.finish())),
			ColumnBuilder::Int1(mut b) => ColumnBuffer::Int1(b.finish()),
			ColumnBuilder::Int2(mut b) => ColumnBuffer::Int2(b.finish()),
			ColumnBuilder::Int4(mut b) => ColumnBuffer::Int4(b.finish()),
			ColumnBuilder::Int8(mut b) => ColumnBuffer::Int8(b.finish()),
			ColumnBuilder::Int16(mut b) => ColumnBuffer::Int16(with_int16_type(b.finish())),
			ColumnBuilder::Uint1(mut b) => ColumnBuffer::Uint1(b.finish()),
			ColumnBuilder::Uint2(mut b) => ColumnBuffer::Uint2(b.finish()),
			ColumnBuilder::Uint4(mut b) => ColumnBuffer::Uint4(b.finish()),
			ColumnBuilder::Uint8(mut b) => ColumnBuffer::Uint8(b.finish()),
			ColumnBuilder::Uint16(mut b) => ColumnBuffer::Uint16(with_uint16_type(b.finish())),
			ColumnBuilder::Float4(mut b) => ColumnBuffer::Float4(b.finish()),
			ColumnBuilder::Float8(mut b) => ColumnBuffer::Float8(b.finish()),
			ColumnBuilder::Date(mut b) => ColumnBuffer::Date(b.finish()),
			ColumnBuilder::DateTime(mut b) => ColumnBuffer::DateTime(b.finish()),
			ColumnBuilder::Time(mut b) => ColumnBuffer::Time(b.finish()),
			ColumnBuilder::Duration(mut b) => ColumnBuffer::Duration(b.finish()),
			ColumnBuilder::IdentityId(b) => ColumnBuffer::IdentityId(uuid_array::from_buffer(b)),
			ColumnBuilder::Uuid4(b) => ColumnBuffer::Uuid4(uuid_array::from_buffer(b)),
			ColumnBuilder::Uuid7(b) => ColumnBuffer::Uuid7(uuid_array::from_buffer(b)),
			ColumnBuilder::DictionaryId {
				buffer,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: dictionary_array::from_buffer(buffer),
				dictionary_id,
			},
			ColumnBuilder::Utf8 {
				mut builder,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: builder.finish(),
				max_bytes,
			},
			ColumnBuilder::Blob {
				mut builder,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: builder.finish(),
				max_bytes,
			},
			ColumnBuilder::Option {
				inner,
				mut bitvec,
			} => ColumnBuffer::Option {
				inner: Box::new(inner.finish()),
				bitvec: bitvec.finish(),
			},
			ColumnBuilder::Buffer(mut buffer) => {
				buffer.freeze();
				buffer
			}
		}
	}
}

impl ColumnBuffer {
	pub fn into_builder(self) -> ColumnBuilder {
		match self {
			ColumnBuffer::Bool(a) => ColumnBuilder::Bool(boolean_builder(a.into_parts().0)),
			ColumnBuffer::Int1(a) => ColumnBuilder::Int1(primitive_builder(a)),
			ColumnBuffer::Int2(a) => ColumnBuilder::Int2(primitive_builder(a)),
			ColumnBuffer::Int4(a) => ColumnBuilder::Int4(primitive_builder(a)),
			ColumnBuffer::Int8(a) => ColumnBuilder::Int8(primitive_builder(a)),
			ColumnBuffer::Int16(a) => ColumnBuilder::Int16(primitive_builder(a)),
			ColumnBuffer::Uint1(a) => ColumnBuilder::Uint1(primitive_builder(a)),
			ColumnBuffer::Uint2(a) => ColumnBuilder::Uint2(primitive_builder(a)),
			ColumnBuffer::Uint4(a) => ColumnBuilder::Uint4(primitive_builder(a)),
			ColumnBuffer::Uint8(a) => ColumnBuilder::Uint8(primitive_builder(a)),
			ColumnBuffer::Uint16(a) => ColumnBuilder::Uint16(primitive_builder(a)),
			ColumnBuffer::Float4(a) => ColumnBuilder::Float4(primitive_builder(a)),
			ColumnBuffer::Float8(a) => ColumnBuilder::Float8(primitive_builder(a)),
			ColumnBuffer::Date(a) => ColumnBuilder::Date(primitive_builder(a)),
			ColumnBuffer::DateTime(a) => ColumnBuilder::DateTime(primitive_builder(a)),
			ColumnBuffer::Time(a) => ColumnBuilder::Time(primitive_builder(a)),
			ColumnBuffer::Duration(a) => ColumnBuilder::Duration(primitive_builder(a)),
			ColumnBuffer::IdentityId(a) => ColumnBuilder::IdentityId(fixed_builder(a)),
			ColumnBuffer::Uuid4(a) => ColumnBuilder::Uuid4(fixed_builder(a)),
			ColumnBuffer::Uuid7(a) => ColumnBuilder::Uuid7(fixed_builder(a)),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuilder::DictionaryId {
				buffer: fixed_builder(container),
				dictionary_id,
			},
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => ColumnBuilder::Utf8 {
				builder: varlen_builder(container),
				max_bytes,
			},
			ColumnBuffer::Blob {
				container,
				max_bytes,
			} => ColumnBuilder::Blob {
				builder: varlen_builder(container),
				max_bytes,
			},
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => ColumnBuilder::Option {
				inner: Box::new(inner.into_builder()),
				bitvec: boolean_builder(bitvec),
			},
			other => ColumnBuilder::Buffer(other),
		}
	}
}

pub(crate) fn primitive_builder<A>(array: PrimitiveArray<A>) -> PrimitiveBuilder<A>
where
	A: ArrowPrimitiveType,
{
	let data_type = array.data_type().clone();
	let builder = match array.into_builder() {
		Ok(builder) => builder,
		Err(array) => {
			let mut builder = PrimitiveBuilder::with_capacity(array.len());
			builder.append_slice(array.values());
			builder
		}
	};
	builder.with_data_type(data_type)
}

pub(crate) fn boolean_builder(bits: BooleanBuffer) -> BooleanBufferBuilder {
	let len = bits.len();
	let bits = if bits.offset() == 0 {
		match bits.into_inner().into_mutable() {
			Ok(buffer) => return BooleanBufferBuilder::new_from_buffer(buffer, len),
			Err(buffer) => BooleanBuffer::new(buffer, 0, len),
		}
	} else {
		bits
	};
	let mut builder = BooleanBufferBuilder::new(len);
	builder.append_buffer(&bits);
	builder
}

pub(crate) fn fixed_builder(array: FixedSizeBinaryArray) -> MutableBuffer {
	let bytes = array.len() * array.value_length() as usize;
	let (_, values, _) = array.into_parts();
	match values.into_mutable() {
		Ok(buffer) => buffer,
		Err(values) => {
			let mut buffer = MutableBuffer::with_capacity(bytes);
			buffer.extend_from_slice(&values.as_slice()[..bytes]);
			buffer
		}
	}
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
