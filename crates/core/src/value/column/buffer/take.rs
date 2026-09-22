// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_value::{
	util::bitmap,
	value::container::{bool_array, dictionary_array, primitive, uuid_array, varlen_array},
};

use crate::value::column::ColumnBuffer;

macro_rules! map_container {
	($self:expr, |$a:ident| $native:expr, |$u:ident| $fixed:expr, |$v:ident| $varlen:expr) => {
		match $self {
			ColumnBuffer::Float4($a) => ColumnBuffer::Float4($native),
			ColumnBuffer::Float8($a) => ColumnBuffer::Float8($native),
			ColumnBuffer::Int1($a) => ColumnBuffer::Int1($native),
			ColumnBuffer::Int2($a) => ColumnBuffer::Int2($native),
			ColumnBuffer::Int4($a) => ColumnBuffer::Int4($native),
			ColumnBuffer::Int8($a) => ColumnBuffer::Int8($native),
			ColumnBuffer::Uint1($a) => ColumnBuffer::Uint1($native),
			ColumnBuffer::Uint2($a) => ColumnBuffer::Uint2($native),
			ColumnBuffer::Uint4($a) => ColumnBuffer::Uint4($native),
			ColumnBuffer::Uint8($a) => ColumnBuffer::Uint8($native),
			ColumnBuffer::Int16($a) => ColumnBuffer::Int16($native),
			ColumnBuffer::Uint16($a) => ColumnBuffer::Uint16($native),
			ColumnBuffer::Bool(_) => {
				unreachable!(
					"map_container! must not be called on Bool variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::Utf8 {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Date($a) => ColumnBuffer::Date($native),
			ColumnBuffer::DateTime($a) => ColumnBuffer::DateTime($native),
			ColumnBuffer::Time($a) => ColumnBuffer::Time($native),
			ColumnBuffer::Duration($a) => ColumnBuffer::Duration($native),

			ColumnBuffer::IdentityId($u) => ColumnBuffer::IdentityId($fixed),
			ColumnBuffer::DictionaryId {
				..
			} => {
				unreachable!(
					"map_container! must not be called on DictionaryId variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::Uuid4($u) => ColumnBuffer::Uuid4($fixed),
			ColumnBuffer::Uuid7($u) => ColumnBuffer::Uuid7($fixed),
			ColumnBuffer::Blob {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Int {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Int {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Uint {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Uint {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Decimal {
				container: $v,
				precision,
				scale,
			} => ColumnBuffer::Decimal {
				container: $varlen,
				precision: *precision,
				scale: *scale,
			},
			ColumnBuffer::Any {
				container: $v,
				declared_type,
			} => ColumnBuffer::Any {
				container: $varlen,
				declared_type: declared_type.clone(),
			},
			ColumnBuffer::Digest {
				container: $v,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: $varlen,
				inner: inner.clone(),
				accuracy: *accuracy,
			},
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
				let new_bitvec = bitmap::take(bitvec, num);

				if !new_bitvec.has_false() && !new_bitvec.is_empty() {
					inner.take(num)
				} else {
					ColumnBuffer::Option {
						inner: Box::new(inner.take(num)),
						bitvec: new_bitvec,
					}
				}
			}
			ColumnBuffer::Bool(a) => ColumnBuffer::Bool(bool_array::take(a, num)),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: dictionary_array::take(container, num),
				dictionary_id: *dictionary_id,
			},
			_ => map_container!(self, |a| primitive::take(a, num), |u| uuid_array::take(u, num), |v| {
				varlen_array::take(v, num)
			}),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> ColumnBuffer {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				assert!(start <= end, "ColumnBuffer::slice: start {start} > end {end}");
				assert!(end <= bitvec.len(), "ColumnBuffer::slice: end {end} > len {}", bitvec.len());
				ColumnBuffer::Option {
					inner: Box::new(inner.slice(start, end)),
					bitvec: bitmap::slice(bitvec, start, end),
				}
			}
			ColumnBuffer::Bool(a) => ColumnBuffer::Bool(bool_array::slice(a, start, end)),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: dictionary_array::slice(container, start, end),
				dictionary_id: *dictionary_id,
			},
			_ => map_container!(
				self,
				|a| primitive::slice(a, start, end),
				|u| uuid_array::slice(u, start, end),
				|v| varlen_array::slice(v, start, end)
			),
		}
	}

	pub fn gather(&self, indices: &[usize]) -> ColumnBuffer {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => ColumnBuffer::Option {
				inner: Box::new(inner.gather(indices)),
				bitvec: BooleanBuffer::collect_bool(indices.len(), |row| bitvec.value(indices[row])),
			},
			_ => {
				let mut cloned = self.clone();
				cloned.reorder(indices);
				cloned
			}
		}
	}
}
