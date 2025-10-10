// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{diagnostic::engine, return_error};

use crate::value::{
	column::ColumnData,
	container::{BlobContainer, BoolContainer, NumberContainer, TemporalContainer, Utf8Container, UuidContainer},
};

impl ColumnData {
	pub fn extend(&mut self, other: ColumnData) -> crate::Result<()> {
		match (&mut *self, other) {
			// Same type extensions - delegate to container extend
			// method
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
			(ColumnData::Interval(l), ColumnData::Interval(r)) => l.extend(&r)?,
			(ColumnData::RowNumber(l), ColumnData::RowNumber(r)) => l.extend(&r)?,
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
			(ColumnData::Undefined(l), ColumnData::Undefined(r)) => l.extend(&r)?,

			// Promote Undefined to typed
			(ColumnData::Undefined(l_container), typed_r) => {
				let l_len = l_container.len();
				match typed_r {
					ColumnData::Bool(r) => {
						let mut new_container = BoolContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Bool(new_container);
					}
					ColumnData::Float4(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Float4(new_container);
					}
					ColumnData::Float8(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Float8(new_container);
					}
					ColumnData::Int1(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int1(new_container);
					}
					ColumnData::Int2(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int2(new_container);
					}
					ColumnData::Int4(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int4(new_container);
					}
					ColumnData::Int8(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int8(new_container);
					}
					ColumnData::Int16(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int16(new_container);
					}
					ColumnData::Uint1(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint1(new_container);
					}
					ColumnData::Uint2(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint2(new_container);
					}
					ColumnData::Uint4(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint4(new_container);
					}
					ColumnData::Uint8(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint8(new_container);
					}
					ColumnData::Uint16(r) => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint16(new_container);
					}
					ColumnData::Utf8 {
						container: r,
						max_bytes,
					} => {
						let mut new_container = Utf8Container::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Utf8 {
							container: new_container,
							max_bytes,
						};
					}
					ColumnData::Date(r) => {
						let mut new_container =
							TemporalContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Date(new_container);
					}
					ColumnData::DateTime(r) => {
						let mut new_container =
							TemporalContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::DateTime(new_container);
					}
					ColumnData::Time(r) => {
						let mut new_container =
							TemporalContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Time(new_container);
					}
					ColumnData::Interval(r) => {
						let mut new_container =
							TemporalContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Interval(new_container);
					}
					ColumnData::Uuid4(r) => {
						let mut new_container = UuidContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uuid4(new_container);
					}
					ColumnData::Uuid7(r) => {
						let mut new_container = UuidContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uuid7(new_container);
					}
					ColumnData::Blob {
						container: r,
						max_bytes,
					} => {
						let mut new_container = BlobContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Blob {
							container: new_container,
							max_bytes,
						};
					}
					ColumnData::Int {
						container: r,
						max_bytes,
					} => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Int {
							container: new_container,
							max_bytes,
						};
					}
					ColumnData::Uint {
						container: r,
						max_bytes,
					} => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Uint {
							container: new_container,
							max_bytes,
						};
					}
					ColumnData::Decimal {
						container: r,
						precision,
						scale,
					} => {
						let mut new_container = NumberContainer::with_capacity(l_len + r.len());
						new_container.extend_from_undefined(l_len);
						new_container.extend(&r)?;
						*self = ColumnData::Decimal {
							container: new_container,
							precision,
							scale,
						};
					}
					ColumnData::RowNumber(_) => {
						return_error!(engine::frame_error(
							"Cannot extend RowNumber column from Undefined".to_string()
						));
					}
					ColumnData::IdentityId(_) => {
						return_error!(engine::frame_error(
							"Cannot extend IdentityId column from Undefined".to_string()
						));
					}
					ColumnData::Undefined(_) => {}
					ColumnData::Any(_) => {
						unreachable!("Any type not supported in extend operations");
					}
				}
			}

			// Extend typed with Undefined
			(typed_l, ColumnData::Undefined(r_container)) => {
				let r_len = r_container.len();
				match typed_l {
					ColumnData::Bool(l) => l.extend_from_undefined(r_len),
					ColumnData::Float4(l) => l.extend_from_undefined(r_len),
					ColumnData::Float8(l) => l.extend_from_undefined(r_len),
					ColumnData::Int1(l) => l.extend_from_undefined(r_len),
					ColumnData::Int2(l) => l.extend_from_undefined(r_len),
					ColumnData::Int4(l) => l.extend_from_undefined(r_len),
					ColumnData::Int8(l) => l.extend_from_undefined(r_len),
					ColumnData::Int16(l) => l.extend_from_undefined(r_len),
					ColumnData::Uint1(l) => l.extend_from_undefined(r_len),
					ColumnData::Uint2(l) => l.extend_from_undefined(r_len),
					ColumnData::Uint4(l) => l.extend_from_undefined(r_len),
					ColumnData::Uint8(l) => l.extend_from_undefined(r_len),
					ColumnData::Uint16(l) => l.extend_from_undefined(r_len),
					ColumnData::Utf8 {
						container: l,
						..
					} => l.extend_from_undefined(r_len),
					ColumnData::Date(l) => l.extend_from_undefined(r_len),
					ColumnData::DateTime(l) => l.extend_from_undefined(r_len),
					ColumnData::Time(l) => l.extend_from_undefined(r_len),
					ColumnData::Interval(l) => l.extend_from_undefined(r_len),
					ColumnData::RowNumber(l) => l.extend_from_undefined(r_len),
					ColumnData::IdentityId(l) => l.extend_from_undefined(r_len),
					ColumnData::Uuid4(l) => l.extend_from_undefined(r_len),
					ColumnData::Uuid7(l) => l.extend_from_undefined(r_len),
					ColumnData::Blob {
						container: l,
						..
					} => l.extend_from_undefined(r_len),
					ColumnData::Int {
						container: l,
						..
					} => l.extend_from_undefined(r_len),
					ColumnData::Uint {
						container: l,
						..
					} => l.extend_from_undefined(r_len),
					ColumnData::Decimal {
						container: l,
						..
					} => l.extend_from_undefined(r_len),
					ColumnData::Undefined(_) => {
						unreachable!()
					}
					&mut ColumnData::Any(_) => {
						unreachable!("Any type not supported in extend operations");
					}
				}
			}

			// Type mismatch
			(_, _) => {
				return_error!(engine::frame_error("column type mismatch".to_string()));
			}
		}

		Ok(())
	}
}
