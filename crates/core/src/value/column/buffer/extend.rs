// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_array::{
	ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray, GenericByteArray, PrimitiveArray,
	builder::{GenericByteBuilder, PrimitiveBuilder},
	types::ByteArrayType,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer, ScalarBuffer};
use reifydb_value::{
	Result,
	value::container::{
		dictionary_array::{self, DICTIONARY_ENTRY_WIDTH},
		uuid_array::{self, UUID_WIDTH},
		varlen_array,
	},
};

use crate::{
	return_internal_error,
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
		_ => with_container!(
			buffer,
			|c| {
				for _ in 0..count {
					c.push_default();
				}
			},
			|a| extend_native(a, |b| b.append_value_n(Default::default(), count)),
			|t| extend_native(t, |b| b.append_value_n(Default::default(), count)),
			|u| extend_fixed(u, |b| b.extend_zeros(count * UUID_WIDTH)),
			|v| extend_varlen(v, |b| append_empty(b, count))
		),
	}
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
			) => l.extend(&r)?,
			(
				ColumnBuffer::Uint {
					container: l,
					..
				},
				ColumnBuffer::Uint {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(
				ColumnBuffer::Decimal {
					container: l,
					..
				},
				ColumnBuffer::Decimal {
					container: r,
					..
				},
			) => l.extend(&r)?,
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
			(ColumnBuffer::Any(l), ColumnBuffer::Any(r)) => l.extend(&r)?,
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
			) if *l_inner == r_inner && *l_accuracy == r_accuracy => l.extend(&r)?,

			(
				ColumnBuffer::Option {
					inner: l_inner,
					bitvec: l_bitvec,
				},
				ColumnBuffer::Option {
					inner: r_inner,
					bitvec: r_bitvec,
				},
			) => {
				if l_inner.get_type() == r_inner.get_type() {
					l_inner.extend(*r_inner)?;
				} else if !r_bitvec.has_true() {
					let r_len = r_inner.len();
					push_defaults(l_inner.as_mut(), r_len);
				} else if !l_bitvec.has_true() {
					let l_len = l_inner.len();
					let r_type = r_inner.get_type();
					let (mut new_inner, _) =
						ColumnBuffer::none_typed(r_type, l_len).into_unwrap_option();
					new_inner.extend(*r_inner)?;
					**l_inner = new_inner;
				} else {
					return_internal_error!("column type mismatch in Option extend");
				}
				extend_bits(l_bitvec, |b| b.append_buffer(&r_bitvec));
			}

			(
				ColumnBuffer::Option {
					inner,
					bitvec,
				},
				other,
			) => {
				let other_len = other.len();
				if inner.get_type() != other.get_type() && !bitvec.has_true() {
					let l_len = inner.len();
					let r_type = other.get_type();
					let (mut new_inner, _) =
						ColumnBuffer::none_typed(r_type, l_len).into_unwrap_option();
					new_inner.extend(other)?;
					**inner = new_inner;
				} else {
					inner.extend(other)?;
				}
				extend_bits(bitvec, |b| b.append_n(other_len, true));
			}

			(
				_,
				ColumnBuffer::Option {
					inner: r_inner,
					bitvec: r_bitvec,
				},
			) => {
				let l_len = self.len();
				let r_len = r_inner.len();
				let mut l_bits = BooleanBufferBuilder::new(l_len + r_bitvec.len());
				l_bits.append_n(l_len, true);
				l_bits.append_buffer(&r_bitvec);
				let l_bitvec = l_bits.finish();
				let inner = mem::replace(self, ColumnBuffer::bool(vec![]));
				let mut boxed_inner = Box::new(inner);

				if boxed_inner.get_type() != r_inner.get_type() && !r_bitvec.has_true() {
					push_defaults(boxed_inner.as_mut(), r_len);
				} else {
					boxed_inner.extend(*r_inner)?;
				}

				*self = ColumnBuffer::Option {
					inner: boxed_inner,
					bitvec: l_bitvec,
				};
			}

			(_, _) => {
				return_internal_error!("column type mismatch");
			}
		}

		Ok(())
	}
}
