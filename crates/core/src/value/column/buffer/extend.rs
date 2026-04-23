// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use reifydb_type::{Result, storage::DataBitVec, util::bitvec::BitVec};

use crate::{
	return_internal_error,
	value::column::{ColumnBuffer, buffer::with_container},
};

impl ColumnBuffer {
	pub fn extend(&mut self, other: ColumnBuffer) -> Result<()> {
		match (&mut *self, other) {
			// Same type extensions
			(ColumnBuffer::Bool(l), ColumnBuffer::Bool(r)) => l.extend(&r)?,
			(ColumnBuffer::Float4(l), ColumnBuffer::Float4(r)) => l.extend(&r)?,
			(ColumnBuffer::Float8(l), ColumnBuffer::Float8(r)) => l.extend(&r)?,
			(ColumnBuffer::Int1(l), ColumnBuffer::Int1(r)) => l.extend(&r)?,
			(ColumnBuffer::Int2(l), ColumnBuffer::Int2(r)) => l.extend(&r)?,
			(ColumnBuffer::Int4(l), ColumnBuffer::Int4(r)) => l.extend(&r)?,
			(ColumnBuffer::Int8(l), ColumnBuffer::Int8(r)) => l.extend(&r)?,
			(ColumnBuffer::Int16(l), ColumnBuffer::Int16(r)) => l.extend(&r)?,
			(ColumnBuffer::Uint1(l), ColumnBuffer::Uint1(r)) => l.extend(&r)?,
			(ColumnBuffer::Uint2(l), ColumnBuffer::Uint2(r)) => l.extend(&r)?,
			(ColumnBuffer::Uint4(l), ColumnBuffer::Uint4(r)) => l.extend(&r)?,
			(ColumnBuffer::Uint8(l), ColumnBuffer::Uint8(r)) => l.extend(&r)?,
			(ColumnBuffer::Uint16(l), ColumnBuffer::Uint16(r)) => l.extend(&r)?,
			(
				ColumnBuffer::Utf8 {
					container: l,
					..
				},
				ColumnBuffer::Utf8 {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(ColumnBuffer::Date(l), ColumnBuffer::Date(r)) => l.extend(&r)?,
			(ColumnBuffer::DateTime(l), ColumnBuffer::DateTime(r)) => l.extend(&r)?,
			(ColumnBuffer::Time(l), ColumnBuffer::Time(r)) => l.extend(&r)?,
			(ColumnBuffer::Duration(l), ColumnBuffer::Duration(r)) => l.extend(&r)?,
			(ColumnBuffer::IdentityId(l), ColumnBuffer::IdentityId(r)) => l.extend(&r)?,
			(ColumnBuffer::Uuid4(l), ColumnBuffer::Uuid4(r)) => l.extend(&r)?,
			(ColumnBuffer::Uuid7(l), ColumnBuffer::Uuid7(r)) => l.extend(&r)?,
			(
				ColumnBuffer::Blob {
					container: l,
					..
				},
				ColumnBuffer::Blob {
					container: r,
					..
				},
			) => l.extend(&r)?,
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
			(ColumnBuffer::DictionaryId(l), ColumnBuffer::DictionaryId(r)) => l.extend(&r)?,

			// Option + Option: extend inner + bitvec
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
					// Same inner type: normal extend
					l_inner.extend(*r_inner)?;
				} else if DataBitVec::count_ones(&r_bitvec) == 0 {
					// Right is all-none with different type: extend left inner with defaults
					let r_len = r_inner.len();
					with_container!(l_inner.as_mut(), |c| {
						for _ in 0..r_len {
							c.push_default();
						}
					});
				} else if DataBitVec::count_ones(l_bitvec) == 0 {
					// Left is all-none with different type: replace left inner type to match
					// right's
					let l_len = l_inner.len();
					let r_type = r_inner.get_type();
					let (mut new_inner, _) =
						ColumnBuffer::none_typed(r_type, l_len).into_unwrap_option();
					new_inner.extend(*r_inner)?;
					**l_inner = new_inner;
				} else {
					// Type mismatch with both having defined values
					return_internal_error!("column type mismatch in Option extend");
				}
				DataBitVec::extend_from(l_bitvec, &r_bitvec);
			}

			// Option + bare: extend inner with bare data, extend bitvec with all-true
			(
				ColumnBuffer::Option {
					inner,
					bitvec,
				},
				other,
			) => {
				let other_len = other.len();
				if inner.get_type() != other.get_type() && DataBitVec::count_ones(bitvec) == 0 {
					// Left is all-none with different type: replace inner type to match bare data
					let l_len = inner.len();
					let r_type = other.get_type();
					let (mut new_inner, _) =
						ColumnBuffer::none_typed(r_type, l_len).into_unwrap_option();
					new_inner.extend(other)?;
					**inner = new_inner;
				} else {
					inner.extend(other)?;
				}
				for _ in 0..other_len {
					DataBitVec::push(bitvec, true);
				}
			}

			// bare + Option: promote bare to Option, then extend
			(
				_,
				ColumnBuffer::Option {
					inner: r_inner,
					bitvec: r_bitvec,
				},
			) => {
				let l_len = self.len();
				let r_len = r_inner.len();
				let mut l_bitvec = BitVec::repeat(l_len, true);
				DataBitVec::extend_from(&mut l_bitvec, &r_bitvec);
				let inner = mem::replace(self, ColumnBuffer::bool(vec![]));
				let mut boxed_inner = Box::new(inner);

				if boxed_inner.get_type() != r_inner.get_type()
					&& DataBitVec::count_ones(&r_bitvec) == 0
				{
					// Right is all-none with different type: extend left with defaults
					with_container!(boxed_inner.as_mut(), |c| {
						for _ in 0..r_len {
							c.push_default();
						}
					});
				} else {
					boxed_inner.extend(*r_inner)?;
				}

				*self = ColumnBuffer::Option {
					inner: boxed_inner,
					bitvec: l_bitvec,
				};
			}

			// Type mismatch
			(_, _) => {
				return_internal_error!("column type mismatch");
			}
		}

		Ok(())
	}
}
