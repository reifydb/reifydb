// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
	mem,
	ops::Add,
	sync::Arc,
};

pub use reifydb_macro::HeapSize;
use reifydb_value::{
	byte_size::ByteSize,
	count::Count,
	util::hash::Hash128,
	value::{
		Value,
		date::Date,
		datetime::DateTime,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		row_number::RowNumber,
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};

pub trait HeapSize {
	fn heap_size(&self) -> usize;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateMemory {
	pub entries: Count,
	pub bytes: ByteSize,
}

impl StateMemory {
	pub const ZERO: Self = Self {
		entries: Count::ZERO,
		bytes: ByteSize::ZERO,
	};

	pub fn new(entries: Count, bytes: ByteSize) -> Self {
		Self {
			entries,
			bytes,
		}
	}
}

impl Add for StateMemory {
	type Output = Self;

	fn add(self, rhs: Self) -> Self {
		Self {
			entries: self.entries + rhs.entries,
			bytes: self.bytes + rhs.bytes,
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorSample {
	pub memory: Option<StateMemory>,
	pub row_number_cache: Option<StateMemory>,
}

impl OperatorSample {
	pub fn with_memory(memory: StateMemory) -> Self {
		Self {
			memory: Some(memory),
			row_number_cache: None,
		}
	}

	pub fn with_row_number_cache(mut self, memory: StateMemory) -> Self {
		self.row_number_cache = Some(memory);
		self
	}
}

macro_rules! zero_heap {
	($($ty:ty),* $(,)?) => {
		$(impl HeapSize for $ty {
			fn heap_size(&self) -> usize {
				0
			}
		})*
	};
}

zero_heap!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, char, ());
zero_heap!(OrderedF32, OrderedF64, Date, DateTime, Time, Duration, IdentityId, Uuid4, Uuid7, RowNumber, Hash128);

const BIGNUM_APPROX_HEAP: usize = 32;
const BTREE_ENTRY_OVERHEAD: usize = 16;

impl HeapSize for String {
	fn heap_size(&self) -> usize {
		self.capacity()
	}
}

impl<T: HeapSize> HeapSize for Option<T> {
	fn heap_size(&self) -> usize {
		self.as_ref().map_or(0, HeapSize::heap_size)
	}
}

impl<T: HeapSize> HeapSize for Vec<T> {
	fn heap_size(&self) -> usize {
		self.capacity() * mem::size_of::<T>() + self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl<T: HeapSize> HeapSize for VecDeque<T> {
	fn heap_size(&self) -> usize {
		self.capacity() * mem::size_of::<T>() + self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl<T: HeapSize> HeapSize for Box<T> {
	fn heap_size(&self) -> usize {
		mem::size_of::<T>() + (**self).heap_size()
	}
}

impl<T: HeapSize> HeapSize for Arc<T> {
	fn heap_size(&self) -> usize {
		mem::size_of::<usize>() * 2 + mem::size_of::<T>() + (**self).heap_size()
	}
}

impl HeapSize for Arc<str> {
	fn heap_size(&self) -> usize {
		mem::size_of::<usize>() * 2 + self.len()
	}
}

impl<T: HeapSize> HeapSize for Arc<[T]> {
	fn heap_size(&self) -> usize {
		mem::size_of::<usize>() * 2
			+ self.len() * mem::size_of::<T>()
			+ self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl<T: HeapSize> HeapSize for Box<[T]> {
	fn heap_size(&self) -> usize {
		self.len() * mem::size_of::<T>() + self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl HeapSize for Box<str> {
	fn heap_size(&self) -> usize {
		self.len()
	}
}

impl<K: HeapSize, V: HeapSize> HeapSize for BTreeMap<K, V> {
	fn heap_size(&self) -> usize {
		self.len() * (mem::size_of::<K>() + mem::size_of::<V>() + BTREE_ENTRY_OVERHEAD)
			+ self.iter().map(|(k, v)| k.heap_size() + v.heap_size()).sum::<usize>()
	}
}

impl<T: HeapSize> HeapSize for BTreeSet<T> {
	fn heap_size(&self) -> usize {
		self.len() * (mem::size_of::<T>() + BTREE_ENTRY_OVERHEAD)
			+ self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl<K: HeapSize, V: HeapSize, S> HeapSize for HashMap<K, V, S> {
	fn heap_size(&self) -> usize {
		self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>() + 1)
			+ self.iter().map(|(k, v)| k.heap_size() + v.heap_size()).sum::<usize>()
	}
}

impl<T: HeapSize, S> HeapSize for HashSet<T, S> {
	fn heap_size(&self) -> usize {
		self.capacity() * (mem::size_of::<T>() + 1) + self.iter().map(HeapSize::heap_size).sum::<usize>()
	}
}

impl<A: HeapSize, B: HeapSize> HeapSize for (A, B) {
	fn heap_size(&self) -> usize {
		self.0.heap_size() + self.1.heap_size()
	}
}

impl<A: HeapSize, B: HeapSize, C: HeapSize> HeapSize for (A, B, C) {
	fn heap_size(&self) -> usize {
		self.0.heap_size() + self.1.heap_size() + self.2.heap_size()
	}
}

impl HeapSize for Value {
	fn heap_size(&self) -> usize {
		match self {
			Value::Utf8(text) => text.capacity(),
			Value::Blob(blob) => blob.as_bytes().len(),
			Value::Int(_) | Value::Uint(_) | Value::Decimal(_) => BIGNUM_APPROX_HEAP,
			Value::Any(inner) => mem::size_of::<Value>() + inner.heap_size(),
			Value::List(items) | Value::Tuple(items) => items.heap_size(),
			Value::Record(fields) => {
				fields.capacity() * mem::size_of::<(String, Value)>()
					+ fields.iter()
						.map(|(name, value)| name.capacity() + value.heap_size())
						.sum::<usize>()
			}
			_ => 0,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::HeapSize;

	// The derive must sum heap_size over every field so that adding a
	// heap-owning field later is picked up automatically, and scalar
	// fields must contribute zero.
	#[derive(HeapSize)]
	struct DerivedSample {
		name: String,
		values: Vec<u64>,
		count: u64,
	}

	#[test]
	fn derived_heap_size_sums_all_fields() {
		let sample = DerivedSample {
			name: String::with_capacity(32),
			values: Vec::with_capacity(4),
			count: 7,
		};
		assert_eq!(sample.count, 7);
		assert_eq!(sample.heap_size(), 32 + 4 * 8);
	}
}
