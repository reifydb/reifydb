// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::storage::DataBitVec;

use crate::value::column::ColumnData;

macro_rules! map_container {
	($self:expr, |$c:ident| $body:expr) => {
		match $self {
			ColumnData::Bool($c) => ColumnData::Bool($body),
			ColumnData::Float4($c) => ColumnData::Float4($body),
			ColumnData::Float8($c) => ColumnData::Float8($body),
			ColumnData::Int1($c) => ColumnData::Int1($body),
			ColumnData::Int2($c) => ColumnData::Int2($body),
			ColumnData::Int4($c) => ColumnData::Int4($body),
			ColumnData::Int8($c) => ColumnData::Int8($body),
			ColumnData::Int16($c) => ColumnData::Int16($body),
			ColumnData::Uint1($c) => ColumnData::Uint1($body),
			ColumnData::Uint2($c) => ColumnData::Uint2($body),
			ColumnData::Uint4($c) => ColumnData::Uint4($body),
			ColumnData::Uint8($c) => ColumnData::Uint8($body),
			ColumnData::Uint16($c) => ColumnData::Uint16($body),
			ColumnData::Utf8 {
				container: $c,
				max_bytes,
			} => ColumnData::Utf8 {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnData::Date($c) => ColumnData::Date($body),
			ColumnData::DateTime($c) => ColumnData::DateTime($body),
			ColumnData::Time($c) => ColumnData::Time($body),
			ColumnData::Duration($c) => ColumnData::Duration($body),

			ColumnData::IdentityId($c) => ColumnData::IdentityId($body),
			ColumnData::DictionaryId($c) => ColumnData::DictionaryId($body),
			ColumnData::Uuid4($c) => ColumnData::Uuid4($body),
			ColumnData::Uuid7($c) => ColumnData::Uuid7($body),
			ColumnData::Blob {
				container: $c,
				max_bytes,
			} => ColumnData::Blob {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnData::Int {
				container: $c,
				max_bytes,
			} => ColumnData::Int {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnData::Uint {
				container: $c,
				max_bytes,
			} => ColumnData::Uint {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnData::Decimal {
				container: $c,
				precision,
				scale,
			} => ColumnData::Decimal {
				container: $body,
				precision: *precision,
				scale: *scale,
			},
			ColumnData::Any($c) => ColumnData::Any($body),
			ColumnData::Option {
				..
			} => {
				unreachable!(
					"map_container! must not be called on Option variant directly; handle it explicitly"
				)
			}
		}
	};
}

impl ColumnData {
	pub fn take(&self, num: usize) -> ColumnData {
		match self {
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				let new_bitvec = DataBitVec::take(bitvec, num);
				// If all bits in the taken bitvec are set (all defined),
				// unwrap the Option and return the bare inner data.
				if DataBitVec::count_ones(&new_bitvec) == DataBitVec::len(&new_bitvec)
					&& DataBitVec::len(&new_bitvec) > 0
				{
					inner.take(num)
				} else {
					ColumnData::Option {
						inner: Box::new(inner.take(num)),
						bitvec: new_bitvec,
					}
				}
			}
			_ => map_container!(self, |c| c.take(num)),
		}
	}
}
