// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, util::bitvec::BitVec};

use crate::value::column::ColumnBuffer;

macro_rules! map_container {
	($self:expr, |$c:ident| $body:expr) => {
		match $self {
			ColumnBuffer::Bool($c) => ColumnBuffer::Bool($body),
			ColumnBuffer::Float4($c) => ColumnBuffer::Float4($body),
			ColumnBuffer::Float8($c) => ColumnBuffer::Float8($body),
			ColumnBuffer::Int1($c) => ColumnBuffer::Int1($body),
			ColumnBuffer::Int2($c) => ColumnBuffer::Int2($body),
			ColumnBuffer::Int4($c) => ColumnBuffer::Int4($body),
			ColumnBuffer::Int8($c) => ColumnBuffer::Int8($body),
			ColumnBuffer::Int16($c) => ColumnBuffer::Int16($body),
			ColumnBuffer::Uint1($c) => ColumnBuffer::Uint1($body),
			ColumnBuffer::Uint2($c) => ColumnBuffer::Uint2($body),
			ColumnBuffer::Uint4($c) => ColumnBuffer::Uint4($body),
			ColumnBuffer::Uint8($c) => ColumnBuffer::Uint8($body),
			ColumnBuffer::Uint16($c) => ColumnBuffer::Uint16($body),
			ColumnBuffer::Utf8 {
				container: $c,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Date($c) => ColumnBuffer::Date($body),
			ColumnBuffer::DateTime($c) => ColumnBuffer::DateTime($body),
			ColumnBuffer::Time($c) => ColumnBuffer::Time($body),
			ColumnBuffer::Duration($c) => ColumnBuffer::Duration($body),

			ColumnBuffer::IdentityId($c) => ColumnBuffer::IdentityId($body),
			ColumnBuffer::DictionaryId($c) => ColumnBuffer::DictionaryId($body),
			ColumnBuffer::Uuid4($c) => ColumnBuffer::Uuid4($body),
			ColumnBuffer::Uuid7($c) => ColumnBuffer::Uuid7($body),
			ColumnBuffer::Blob {
				container: $c,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Int {
				container: $c,
				max_bytes,
			} => ColumnBuffer::Int {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Uint {
				container: $c,
				max_bytes,
			} => ColumnBuffer::Uint {
				container: $body,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Decimal {
				container: $c,
				precision,
				scale,
			} => ColumnBuffer::Decimal {
				container: $body,
				precision: *precision,
				scale: *scale,
			},
			ColumnBuffer::Any($c) => ColumnBuffer::Any($body),
			ColumnBuffer::Option {
				..
			} => {
				unreachable!(
					"map_container! must not be called on Option variant directly; handle it explicitly"
				)
			}
		}
	};
}

impl ColumnBuffer {
	pub fn take(&self, num: usize) -> ColumnBuffer {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				let new_bitvec = DataBitVec::take(bitvec, num);

				if DataBitVec::count_ones(&new_bitvec) == DataBitVec::len(&new_bitvec)
					&& DataBitVec::len(&new_bitvec) > 0
				{
					inner.take(num)
				} else {
					ColumnBuffer::Option {
						inner: Box::new(inner.take(num)),
						bitvec: new_bitvec,
					}
				}
			}
			_ => map_container!(self, |c| c.take(num)),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> ColumnBuffer {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				let len = end - start;
				let mut new_bits = Vec::with_capacity(len);
				for row in start..end {
					new_bits.push(DataBitVec::get(bitvec, row));
				}
				let new_bitvec = BitVec::from(new_bits);
				ColumnBuffer::Option {
					inner: Box::new(inner.slice(start, end)),
					bitvec: new_bitvec,
				}
			}
			_ => map_container!(self, |c| c.slice(start, end)),
		}
	}

	pub fn gather(&self, indices: &[usize]) -> ColumnBuffer {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				let mut new_bits = Vec::with_capacity(indices.len());
				for &i in indices {
					new_bits.push(DataBitVec::get(bitvec, i));
				}
				let new_bitvec = BitVec::from(new_bits);
				ColumnBuffer::Option {
					inner: Box::new(inner.gather(indices)),
					bitvec: new_bitvec,
				}
			}
			_ => {
				let mut cloned = self.clone();
				cloned.reorder(indices);
				cloned
			}
		}
	}
}
