// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_array::{
	ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray, GenericByteArray, PrimitiveArray,
	builder::{GenericByteBuilder, PrimitiveBuilder},
	types::ByteArrayType,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer, NullBuffer, ScalarBuffer};
use reifydb_value::{
	Result,
	value::{
		Value,
		container::{
			any_array::push_any,
			bignum_array::{push_decimal, push_int, push_uint},
			dictionary_array::{self, DICTIONARY_ENTRY_WIDTH},
			uuid_array::{self, UUID_WIDTH},
			varlen_array,
		},
		decimal::Decimal,
		int::Int,
		uint::Uint,
	},
};

use crate::{
	internal_err, return_internal_error,
	value::column::{
		ColumnBuffer,
		buffer::with_container,
		builder::{append_varlen, boolean_builder, fixed_builder, primitive_builder, varlen_builder},
	},
};

fn extend_native<A>(array: &mut PrimitiveArray<A>, append: impl FnOnce(&mut PrimitiveBuilder<A>))
where
	A: ArrowPrimitiveType,
{
	let mut builder =
		primitive_builder(mem::replace(array, PrimitiveArray::new(ScalarBuffer::from(Vec::new()), None)));
	append(&mut builder);
	*array = builder.finish();
}

fn extend_bits(bits: &mut BooleanBuffer, append: impl FnOnce(&mut BooleanBufferBuilder)) {
	let mut builder = boolean_builder(mem::replace(bits, BooleanBuffer::new_unset(0)));
	append(&mut builder);
	*bits = builder.finish();
}

fn extend_bool(array: &mut BooleanArray, append: impl FnOnce(&mut BooleanBufferBuilder)) {
	let (mut bits, _) = mem::replace(array, BooleanArray::from(BooleanBuffer::new_unset(0))).into_parts();
	extend_bits(&mut bits, append);
	*array = BooleanArray::from(bits);
}

fn extend_fixed(array: &mut FixedSizeBinaryArray, append: impl FnOnce(&mut MutableBuffer)) {
	let mut buffer = fixed_builder(mem::replace(array, uuid_array::from_buffer(MutableBuffer::new(0))));
	append(&mut buffer);
	*array = uuid_array::from_buffer(buffer);
}

fn extend_dictionary(array: &mut FixedSizeBinaryArray, append: impl FnOnce(&mut MutableBuffer)) {
	let mut buffer = fixed_builder(mem::replace(array, dictionary_array::from_buffer(MutableBuffer::new(0))));
	append(&mut buffer);
	*array = dictionary_array::from_buffer(buffer);
}

fn extend_varlen<T, R>(array: &mut GenericByteArray<T>, append: impl FnOnce(&mut GenericByteBuilder<T>) -> R) -> R
where
	T: ByteArrayType<Offset = i64>,
{
	let mut builder = varlen_builder(mem::replace(array, varlen_array::empty()));
	let appended = append(&mut builder);
	*array = builder.finish();
	appended
}

fn push_defaults(buffer: &mut ColumnBuffer, count: usize) {
	match buffer {
		ColumnBuffer::Bool(a) => extend_bool(a, |b| b.append_n(count, false)),
		ColumnBuffer::Uint16(a) => extend_native(a, |b| b.append_value_n(Default::default(), count)),
		ColumnBuffer::DictionaryId {
			container,
			..
		} => extend_dictionary(container, |b| b.extend_zeros(count * DICTIONARY_ENTRY_WIDTH)),
		ColumnBuffer::Int {
			container,
			..
		} => extend_varlen(container, |b| {
			for _ in 0..count {
				push_int(b, &Int::default());
			}
		}),
		ColumnBuffer::Uint {
			container,
			..
		} => extend_varlen(container, |b| {
			for _ in 0..count {
				push_uint(b, &Uint::default());
			}
		}),
		ColumnBuffer::Decimal {
			container,
			..
		} => extend_varlen(container, |b| {
			for _ in 0..count {
				push_decimal(b, &Decimal::default());
			}
		}),
		ColumnBuffer::Any {
			container,
			..
		} => extend_varlen(container, |b| {
			for _ in 0..count {
				push_any(b, &Value::none());
			}
		}),
		_ => with_container!(
			buffer,
			|a| extend_native(a, |b| b.append_value_n(Default::default(), count)),
			|t| extend_native(t, |b| b.append_value_n(Default::default(), count)),
			|u| extend_fixed(u, |b| b.extend_zeros(count * UUID_WIDTH)),
			|v| extend_varlen(v, |b| append_empty(b, count))
		),
	}
}

fn retyped(right: ColumnBuffer, len: usize) -> Result<ColumnBuffer> {
	let (mut retyped, _) = ColumnBuffer::none_typed(right.get_type(), len).split_nulls();
	retyped.extend_bare(right)?;
	Ok(retyped)
}

fn append_empty<T>(builder: &mut GenericByteBuilder<T>, count: usize)
where
	T: ByteArrayType<Offset = i64>,
	for<'a> &'a T::Native: Default,
{
	for _ in 0..count {
		builder.append_value(<&T::Native>::default());
	}
}

impl ColumnBuffer {
	pub fn extend(&mut self, other: ColumnBuffer) -> Result<()> {
		if self.nulls().is_none() && other.nulls().is_none() {
			return self.extend_bare(other);
		}
		let (mut left, l_nulls) = mem::replace(self, ColumnBuffer::bool(vec![])).split_nulls();
		let (right, r_nulls) = other.split_nulls();
		let (l_len, r_len) = (left.len(), right.len());
		let l_all_none = l_nulls.as_ref().is_some_and(|nulls| nulls.null_count() == nulls.len());
		let r_all_none = r_nulls.as_ref().is_some_and(|nulls| nulls.null_count() == nulls.len());
		let same_type = left.get_type() == right.get_type();
		let merged = match (l_nulls.is_some(), r_nulls.is_some()) {
			(true, true) if !same_type && r_all_none => {
				push_defaults(&mut left, r_len);
				Ok(())
			}
			(true, _) if !same_type && l_all_none => retyped(right, l_len).map(|column| left = column),
			(true, true) if !same_type => internal_err!("column type mismatch in Option extend"),
			(false, true) if !same_type && r_all_none => {
				push_defaults(&mut left, r_len);
				Ok(())
			}
			_ => left.extend_bare(right),
		};
		if let Err(error) = merged {
			*self = left.replace_nulls(l_nulls);
			return Err(error);
		}
		let mut bits = BooleanBufferBuilder::new(l_len + r_len);
		for (nulls, len) in [(l_nulls, l_len), (r_nulls, r_len)] {
			match nulls {
				Some(nulls) => bits.append_buffer(nulls.inner()),
				None => bits.append_n(len, true),
			}
		}
		*self = left.replace_nulls(Some(NullBuffer::new(bits.finish())));
		Ok(())
	}

	fn extend_bare(&mut self, other: ColumnBuffer) -> Result<()> {
		match (&mut *self, other) {
			(ColumnBuffer::Bool(l), ColumnBuffer::Bool(r)) => {
				extend_bool(l, |b| b.append_buffer(r.values()))
			}
			(ColumnBuffer::Float4(l), ColumnBuffer::Float4(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Float8(l), ColumnBuffer::Float8(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Int1(l), ColumnBuffer::Int1(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Int2(l), ColumnBuffer::Int2(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Int4(l), ColumnBuffer::Int4(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Int8(l), ColumnBuffer::Int8(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Int16(l), ColumnBuffer::Int16(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Uint1(l), ColumnBuffer::Uint1(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Uint2(l), ColumnBuffer::Uint2(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Uint4(l), ColumnBuffer::Uint4(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Uint8(l), ColumnBuffer::Uint8(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Uint16(l), ColumnBuffer::Uint16(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				ColumnBuffer::Utf8 {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(ColumnBuffer::Date(l), ColumnBuffer::Date(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::DateTime(l), ColumnBuffer::DateTime(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Time(l), ColumnBuffer::Time(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => {
				extend_native(l, |b| b.append_slice(r.values()))
			}
			(ColumnBuffer::IdentityId(l), ColumnBuffer::IdentityId(r)) => {
				extend_fixed(l, |b| b.extend_from_slice(r.value_data()))
			}
			(ColumnBuffer::Uuid4(l), ColumnBuffer::Uuid4(r)) => {
				extend_fixed(l, |b| b.extend_from_slice(r.value_data()))
			}
			(ColumnBuffer::Uuid7(l), ColumnBuffer::Uuid7(r)) => {
				extend_fixed(l, |b| b.extend_from_slice(r.value_data()))
			}
			(
				ColumnBuffer::Blob {
					container: l,
					..
				},
				ColumnBuffer::Blob {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(
				ColumnBuffer::Int {
					container: l,
					..
				},
				ColumnBuffer::Int {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(
				ColumnBuffer::Uint {
					container: l,
					..
				},
				ColumnBuffer::Uint {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(
				ColumnBuffer::Decimal {
					container: l,
					..
				},
				ColumnBuffer::Decimal {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(
				ColumnBuffer::DictionaryId {
					container: l,
					..
				},
				ColumnBuffer::DictionaryId {
					container: r,
					..
				},
			) => extend_dictionary(l, |b| b.extend_from_slice(r.value_data())),
			(
				ColumnBuffer::Any {
					container: l,
					..
				},
				ColumnBuffer::Any {
					container: r,
					..
				},
			) => extend_varlen(l, |b| append_varlen(b, &r))?,
			(
				ColumnBuffer::Digest {
					container: l,
					inner: l_inner,
					accuracy: l_accuracy,
				},
				ColumnBuffer::Digest {
					container: r,
					inner: r_inner,
					accuracy: r_accuracy,
				},
			) if *l_inner == r_inner && *l_accuracy == r_accuracy => extend_varlen(l, |b| append_varlen(b, &r))?,

			(_, _) => {
				return_internal_error!("column type mismatch");
			}
		}

		Ok(())
	}
}
