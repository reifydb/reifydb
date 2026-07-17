// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
	mem,
	ops::Add,
	sync::Arc,
};

use reifydb_runtime::sync::mutex::Mutex;
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

pub struct MemorySample {
	pub scope: Cow<'static, str>,
	pub metric: &'static str,
	pub value: f64,
	pub unit: &'static str,
}

impl MemorySample {
	pub fn new(scope: impl Into<Cow<'static, str>>, metric: &'static str, value: f64, unit: &'static str) -> Self {
		Self {
			scope: scope.into(),
			metric,
			value,
			unit,
		}
	}
}

pub use reifydb_macro::HeapSize;

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

pub trait MemoryReporter: Send + Sync {
	fn report(&self, out: &mut Vec<MemorySample>);
}

#[derive(Clone)]
pub struct MemoryRegistry {
	inner: Arc<Mutex<Vec<Arc<dyn MemoryReporter>>>>,
}

impl MemoryRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register(&self, reporter: Arc<dyn MemoryReporter>) {
		self.inner.lock().push(reporter);
	}

	pub fn register_all(&self, reporters: impl IntoIterator<Item = Arc<dyn MemoryReporter>>) {
		self.inner.lock().extend(reporters);
	}

	pub fn collect(&self) -> Vec<MemorySample> {
		let reporters: Vec<Arc<dyn MemoryReporter>> = self.inner.lock().clone();
		let mut out = Vec::new();
		for reporter in &reporters {
			reporter.report(&mut out);
		}
		out
	}
}

impl Default for MemoryRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::{HeapSize, MemoryRegistry, MemoryReporter, MemorySample};

	struct Fixed {
		scope: &'static str,
		bytes: f64,
	}

	impl MemoryReporter for Fixed {
		fn report(&self, out: &mut Vec<MemorySample>) {
			out.push(MemorySample::new(self.scope, "resident_bytes", self.bytes, "bytes"));
		}
	}

	#[test]
	fn collect_gathers_samples_from_every_registered_reporter() {
		let registry = MemoryRegistry::new();
		registry.register(Arc::new(Fixed {
			scope: "a",
			bytes: 10.0,
		}));
		registry.register(Arc::new(Fixed {
			scope: "b",
			bytes: 20.0,
		}));

		let samples = registry.collect();
		assert_eq!(samples.len(), 2, "every registered reporter must contribute its samples");
		let a = samples.iter().find(|s| s.scope == "a").expect("reporter a must appear");
		assert_eq!(a.metric, "resident_bytes");
		assert_eq!(a.value, 10.0);
		assert_eq!(a.unit, "bytes");
		assert!(samples.iter().any(|s| s.scope == "b" && s.value == 20.0), "reporter b must appear");
	}

	#[test]
	fn register_all_adds_every_reporter() {
		let registry = MemoryRegistry::new();
		let reporters: Vec<Arc<dyn MemoryReporter>> = vec![
			Arc::new(Fixed {
				scope: "x",
				bytes: 1.0,
			}),
			Arc::new(Fixed {
				scope: "y",
				bytes: 2.0,
			}),
		];
		registry.register_all(reporters);
		assert_eq!(registry.collect().len(), 2, "register_all must add all reporters at once");
	}

	#[test]
	fn a_clone_shares_the_same_reporter_list() {
		let registry = MemoryRegistry::new();
		let clone = registry.clone();
		clone.register(Arc::new(Fixed {
			scope: "shared",
			bytes: 5.0,
		}));
		assert_eq!(
			registry.collect().len(),
			1,
			"a clone must observe reporters registered through the other handle (shared Arc backing)"
		);
	}

	#[test]
	fn an_empty_registry_collects_nothing() {
		assert!(MemoryRegistry::new().collect().is_empty());
	}

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
