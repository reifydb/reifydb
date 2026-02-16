// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{return_error, storage::DataBitVec, util::bitvec::BitVec};

use crate::{
	error::diagnostic::internal::internal,
	value::column::{ColumnData, data::with_container},
};

impl ColumnData {
	pub fn extend(&mut self, other: ColumnData) -> reifydb_type::Result<()> {
		match (&mut *self, other) {
			// Same type extensions
			(ColumnData::Bool(l), ColumnData::Bool(r)) => l.extend(&r)?,
			(ColumnData::Float4(l), ColumnData::Float4(r)) => l.extend(&r)?,
			(ColumnData::Float8(l), ColumnData::Float8(r)) => l.extend(&r)?,
			(ColumnData::Int1(l), ColumnData::Int1(r)) => l.extend(&r)?,
			(ColumnData::Int2(l), ColumnData::Int2(r)) => l.extend(&r)?,
			(ColumnData::Int4(l), ColumnData::Int4(r)) => l.extend(&r)?,
			(ColumnData::Int8(l), ColumnData::Int8(r)) => l.extend(&r)?,
			(ColumnData::Int16(l), ColumnData::Int16(r)) => l.extend(&r)?,
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => l.extend(&r)?,
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => l.extend(&r)?,
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => l.extend(&r)?,
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => l.extend(&r)?,
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => l.extend(&r)?,
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				ColumnData::Utf8 {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(ColumnData::Date(l), ColumnData::Date(r)) => l.extend(&r)?,
			(ColumnData::DateTime(l), ColumnData::DateTime(r)) => l.extend(&r)?,
			(ColumnData::Time(l), ColumnData::Time(r)) => l.extend(&r)?,
			(ColumnData::Duration(l), ColumnData::Duration(r)) => l.extend(&r)?,
			(ColumnData::IdentityId(l), ColumnData::IdentityId(r)) => l.extend(&r)?,
			(ColumnData::Uuid4(l), ColumnData::Uuid4(r)) => l.extend(&r)?,
			(ColumnData::Uuid7(l), ColumnData::Uuid7(r)) => l.extend(&r)?,
			(
				ColumnData::Blob {
					container: l,
					..
				},
				ColumnData::Blob {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => l.extend(&r)?,
			(ColumnData::DictionaryId(l), ColumnData::DictionaryId(r)) => l.extend(&r)?,

			// Option + Option: extend inner + bitvec
			(
				ColumnData::Option {
					inner: l_inner,
					bitvec: l_bitvec,
				},
				ColumnData::Option {
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
						ColumnData::none_typed(r_type, l_len).into_unwrap_option();
					new_inner.extend(*r_inner)?;
					**l_inner = new_inner;
				} else {
					// Type mismatch with both having defined values
					return_error!(internal("column type mismatch in Option extend".to_string()));
				}
				DataBitVec::extend_from(l_bitvec, &r_bitvec);
			}

			// Option + bare: extend inner with bare data, extend bitvec with all-true
			(
				ColumnData::Option {
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
						ColumnData::none_typed(r_type, l_len).into_unwrap_option();
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
				ColumnData::Option {
					inner: r_inner,
					bitvec: r_bitvec,
				},
			) => {
				let l_len = self.len();
				let r_len = r_inner.len();
				let mut l_bitvec = BitVec::repeat(l_len, true);
				DataBitVec::extend_from(&mut l_bitvec, &r_bitvec);
				let inner = std::mem::replace(self, ColumnData::bool(vec![]));
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

				*self = ColumnData::Option {
					inner: boxed_inner,
					bitvec: l_bitvec,
				};
			}

			// Type mismatch
			(_, _) => {
				return_error!(internal("column type mismatch".to_string()));
			}
		}

		Ok(())
	}
}
