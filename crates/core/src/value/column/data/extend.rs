// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	return_error,
	storage::DataBitVec,
	value::container::{
		blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer, number::NumberContainer,
		temporal::TemporalContainer, undefined::UndefinedContainer, utf8::Utf8Container, uuid::UuidContainer,
	},
};

use crate::{
	error::diagnostic::internal::internal,
	value::column::{ColumnData, data::with_container},
};

macro_rules! impl_extend_promote_undefined {
	($self:expr, $l_len:expr, $typed_r:expr) => {
		match $typed_r {
			ColumnData::Bool(r) => {
				let mut c = BoolContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Bool(c);
			}
			ColumnData::Float4(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Float4); }
			ColumnData::Float8(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Float8); }
			ColumnData::Int1(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Int1); }
			ColumnData::Int2(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Int2); }
			ColumnData::Int4(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Int4); }
			ColumnData::Int8(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Int8); }
			ColumnData::Int16(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Int16); }
			ColumnData::Uint1(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Uint1); }
			ColumnData::Uint2(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Uint2); }
			ColumnData::Uint4(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Uint4); }
			ColumnData::Uint8(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Uint8); }
			ColumnData::Uint16(r) => { impl_extend_promote_undefined!(@number $self, $l_len, r, Uint16); }
			ColumnData::Utf8 { container: r, max_bytes } => {
				let mut c = Utf8Container::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Utf8 { container: c, max_bytes };
			}
			ColumnData::Date(r) => { impl_extend_promote_undefined!(@temporal $self, $l_len, r, Date); }
			ColumnData::DateTime(r) => { impl_extend_promote_undefined!(@temporal $self, $l_len, r, DateTime); }
			ColumnData::Time(r) => { impl_extend_promote_undefined!(@temporal $self, $l_len, r, Time); }
			ColumnData::Duration(r) => { impl_extend_promote_undefined!(@temporal $self, $l_len, r, Duration); }
			ColumnData::Uuid4(r) => { impl_extend_promote_undefined!(@uuid $self, $l_len, r, Uuid4); }
			ColumnData::Uuid7(r) => { impl_extend_promote_undefined!(@uuid $self, $l_len, r, Uuid7); }
			ColumnData::Blob { container: r, max_bytes } => {
				let mut c = BlobContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Blob { container: c, max_bytes };
			}
			ColumnData::Int { container: r, max_bytes } => {
				let mut c = NumberContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Int { container: c, max_bytes };
			}
			ColumnData::Uint { container: r, max_bytes } => {
				let mut c = NumberContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Uint { container: c, max_bytes };
			}
			ColumnData::Decimal { container: r, precision, scale } => {
				let mut c = NumberContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::Decimal { container: c, precision, scale };
			}
			ColumnData::IdentityId(_) => {
				return_error!(internal(
					"Cannot extend IdentityId column from Undefined".to_string()
				));
			}
			ColumnData::DictionaryId(r) => {
				let mut c = DictionaryContainer::with_capacity($l_len + r.len());
				c.extend_from_undefined($l_len);
				c.extend(&r)?;
				*$self = ColumnData::DictionaryId(c);
			}
			ColumnData::Undefined(_) => {}
			ColumnData::Any(_) => {
				unreachable!("Any type not supported in extend operations");
			}
			ColumnData::Option { .. } => {
				unreachable!("Undefined + Option handled before macro invocation")
			}
		}
	};
	(@number $self:expr, $l_len:expr, $r:ident, $variant:ident) => {
		let mut c = NumberContainer::with_capacity($l_len + $r.len());
		c.extend_from_undefined($l_len);
		c.extend(&$r)?;
		*$self = ColumnData::$variant(c);
	};
	(@temporal $self:expr, $l_len:expr, $r:ident, $variant:ident) => {
		let mut c = TemporalContainer::with_capacity($l_len + $r.len());
		c.extend_from_undefined($l_len);
		c.extend(&$r)?;
		*$self = ColumnData::$variant(c);
	};
	(@uuid $self:expr, $l_len:expr, $r:ident, $variant:ident) => {
		let mut c = UuidContainer::with_capacity($l_len + $r.len());
		c.extend_from_undefined($l_len);
		c.extend(&$r)?;
		*$self = ColumnData::$variant(c);
	};
}

impl ColumnData {
	pub fn extend(&mut self, other: ColumnData) -> reifydb_type::Result<()> {
		// Handle Undefined + Option before the main match to avoid macro recursion
		if let ColumnData::Undefined(_) = &*self {
			if let ColumnData::Option {
				inner: r_inner,
				bitvec: r_bitvec,
			} = other
			{
				let l_len = self.len();
				// Promote Undefined by extending with the inner data
				self.extend(*r_inner)?;
				// Wrap the promoted result in Option with the appropriate bitvec
				let mut new_bitvec = DataBitVec::spawn(&r_bitvec, l_len + DataBitVec::len(&r_bitvec));
				for _ in 0..l_len {
					DataBitVec::push(&mut new_bitvec, false);
				}
				DataBitVec::extend_from(&mut new_bitvec, &r_bitvec);
				let promoted =
					std::mem::replace(self, ColumnData::Undefined(UndefinedContainer::new(0)));
				*self = ColumnData::Option {
					inner: Box::new(promoted),
					bitvec: new_bitvec,
				};
				return Ok(());
			}
		}

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
			(ColumnData::Undefined(l), ColumnData::Undefined(r)) => l.extend(&r)?,

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
				l_inner.extend(*r_inner)?;
				DataBitVec::extend_from(l_bitvec, &r_bitvec);
			}

			// Promote Undefined to typed
			(ColumnData::Undefined(l_container), typed_r) => {
				let l_len = l_container.len();
				impl_extend_promote_undefined!(self, l_len, typed_r);
			}

			// Extend typed with Undefined
			(
				ColumnData::Option {
					inner,
					bitvec,
				},
				ColumnData::Undefined(r_container),
			) => {
				let r_len = r_container.len();
				inner.extend(ColumnData::Undefined(r_container))?;
				for _ in 0..r_len {
					DataBitVec::push(bitvec, false);
				}
			}
			(typed_l, ColumnData::Undefined(r_container)) => {
				let r_len = r_container.len();
				with_container!(typed_l, |c| c.extend_from_undefined(r_len));
			}

			// Type mismatch
			(_, _) => {
				return_error!(internal("column type mismatch".to_string()));
			}
		}

		Ok(())
	}
}
