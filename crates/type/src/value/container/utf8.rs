// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	result::Result as StdResult,
	str,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	storage::{Cow, Storage},
	value::{Value, container::varlen::VarlenContainer, r#type::Type},
};

pub struct Utf8Container<S: Storage = Cow> {
	inner: VarlenContainer<S>,
}

impl<S: Storage> Clone for Utf8Container<S> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<S: Storage> Debug for Utf8Container<S>
where
	VarlenContainer<S>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Utf8Container").field("inner", &self.inner).finish()
	}
}

impl<S: Storage> PartialEq for Utf8Container<S>
where
	VarlenContainer<S>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.inner == other.inner
	}
}

impl Serialize for Utf8Container<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		// Postcard wire compat with the previous `Vec<String>` form: the
		// inner VarlenContainer encodes as a sequence of byte slices,
		// which postcard serializes identically to a sequence of strings
		// (length-prefixed length-prefixed bytes).
		self.inner.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Utf8Container<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		let inner = VarlenContainer::deserialize(deserializer)?;
		Ok(Self {
			inner,
		})
	}
}

impl Utf8Container<Cow> {
	pub fn new(data: Vec<String>) -> Self {
		Self::from_vec(data)
	}

	pub fn from_vec(data: Vec<String>) -> Self {
		let inner = VarlenContainer::from_byte_slices(data.iter().map(|s| s.as_bytes()));
		Self {
			inner,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		// Heuristic: assume average ~16 bytes per string for the byte
		// arena. This is just a starting capacity hint; the buffer
		// grows on demand.
		Self {
			inner: VarlenContainer::with_capacity(capacity, capacity * 16),
		}
	}

	/// Reconstruct from a Vec of owned Strings (for compatibility with
	/// previous `from_raw_parts(Vec<String>)`).
	pub fn from_raw_parts(data: Vec<String>) -> Self {
		Self::from_vec(data)
	}

	/// Build directly from contiguous bytes + offsets (zero-copy from the
	/// caller's perspective). Caller must ensure the bytes are valid UTF-8
	/// and offsets are well-formed.
	pub fn from_bytes_offsets(data: Vec<u8>, offsets: Vec<u64>) -> Self {
		debug_assert!(str::from_utf8(&data).is_ok(), "Utf8Container data must be valid UTF-8");
		Self {
			inner: VarlenContainer::from_raw_parts(data, offsets),
		}
	}

	/// Try to decompose into a `Vec<String>` for compatibility with code
	/// paths that need owned strings. Always succeeds (allocates).
	pub fn try_into_raw_parts(self) -> Option<Vec<String>> {
		Some(self.iter().map(|s| s.unwrap().to_string()).collect())
	}
}

impl<S: Storage> Utf8Container<S> {
	pub fn from_inner(inner: VarlenContainer<S>) -> Self {
		Self {
			inner,
		}
	}

	/// Construct from storage-generic data+offsets vectors. Used by arena
	/// conversion. Caller must ensure data is valid UTF-8 and offsets are
	/// well-formed (length >= 1, [0] == 0, monotonic non-decreasing,
	/// last <= data.len()).
	pub fn from_storage_parts(data: S::Vec<u8>, offsets: S::Vec<u64>) -> Self {
		Self {
			inner: VarlenContainer::from_storage_parts(data, offsets),
		}
	}

	/// Borrow the inner data + offsets vectors.
	pub fn data_storage(&self) -> &S::Vec<u8> {
		self.inner.data()
	}

	pub fn offsets_storage(&self) -> &S::Vec<u64> {
		self.inner.offsets_data()
	}

	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn capacity(&self) -> usize {
		self.inner.capacity()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.is_empty()
	}

	/// Storage-generic clear (works for any `S: Storage`).
	pub fn clear(&mut self) {
		self.inner.clear_generic();
	}

	/// Borrow the i-th string. UTF-8 validity is guaranteed by construction.
	pub fn get(&self, index: usize) -> Option<&str> {
		let bytes = self.inner.get_bytes(index)?;
		// SAFETY: All push paths validate UTF-8 (push(&str) takes a
		// validated &str; from_bytes_offsets debug-asserts validity).
		// VarlenContainer never splits or rearranges bytes.
		Some(unsafe { str::from_utf8_unchecked(bytes) })
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn is_fully_defined(&self) -> bool {
		true
	}

	/// Borrow the underlying concatenated payload bytes. Used by the FFI
	/// marshal path for zero-copy borrow.
	pub fn data_bytes(&self) -> &[u8] {
		self.inner.data_bytes()
	}

	/// Borrow the underlying offsets array (length = `len + 1`).
	pub fn offsets(&self) -> &[u64] {
		self.inner.offsets()
	}

	/// Borrow the underlying VarlenContainer (test/debug only).
	pub fn inner(&self) -> &VarlenContainer<S> {
		&self.inner
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(str::to_string).unwrap_or_else(|| "none".to_string())
	}

	pub fn get_value(&self, index: usize) -> Value {
		match self.get(index) {
			Some(s) => Value::Utf8(s.to_string()),
			None => Value::none_of(Type::Utf8),
		}
	}

	/// Iterate strings as `Option<&str>`. Always Some for indices < len.
	pub fn iter(&self) -> impl Iterator<Item = Option<&str>> + '_ {
		(0..self.len()).map(|i| self.get(i))
	}

	/// Iterate strings as `&str` directly.
	pub fn iter_str(&self) -> impl Iterator<Item = &str> + '_ {
		(0..self.len()).map(|i| self.get(i).unwrap())
	}
}

impl Utf8Container<Cow> {
	pub fn push(&mut self, value: String) {
		self.inner.push_bytes(value.as_bytes());
	}

	pub fn push_str(&mut self, value: &str) {
		self.inner.push_bytes(value.as_bytes());
	}

	pub fn push_default(&mut self) {
		self.inner.push_bytes(&[]);
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.inner.extend_from(&other.inner);
		Ok(())
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			inner: self.inner.slice(start, end),
		}
	}

	pub fn filter(&mut self, mask: &<Cow as Storage>::BitVec) {
		let bits: Vec<bool> = mask.iter().collect();
		self.inner.filter_in_place(|i| bits.get(i).copied().unwrap_or(false));
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		self.inner.reorder_in_place(indices);
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			inner: self.inner.take_n(num),
		}
	}
}

impl Default for Utf8Container<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use postcard::to_allocvec as postcard_to_allocvec;

	use super::*;
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_new() {
		let data = vec!["hello".to_string(), "world".to_string(), "test".to_string()];
		let container = Utf8Container::new(data.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some("hello"));
		assert_eq!(container.get(1), Some("world"));
		assert_eq!(container.get(2), Some("test"));
	}

	#[test]
	fn test_from_vec() {
		let data = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
		let container = Utf8Container::from_vec(data);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some("foo"));
		assert_eq!(container.get(1), Some("bar"));
		assert_eq!(container.get(2), Some("baz"));

		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container = Utf8Container::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push() {
		let mut container = Utf8Container::with_capacity(3);

		container.push("first".to_string());
		container.push("second".to_string());
		container.push_default();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some("first"));
		assert_eq!(container.get(1), Some("second"));
		assert_eq!(container.get(2), Some(""));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = Utf8Container::from_vec(vec!["a".to_string(), "b".to_string()]);
		let container2 = Utf8Container::from_vec(vec!["c".to_string(), "d".to_string()]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 4);
		assert_eq!(container1.get(0), Some("a"));
		assert_eq!(container1.get(1), Some("b"));
		assert_eq!(container1.get(2), Some("c"));
		assert_eq!(container1.get(3), Some("d"));
	}

	#[test]
	fn test_iter() {
		let data = vec!["x".to_string(), "y".to_string(), "z".to_string()];
		let container = Utf8Container::new(data);

		let collected: Vec<Option<&str>> = container.iter().collect();
		assert_eq!(collected, vec![Some("x"), Some("y"), Some("z")]);
	}

	#[test]
	fn test_slice() {
		let container = Utf8Container::from_vec(vec![
			"one".to_string(),
			"two".to_string(),
			"three".to_string(),
			"four".to_string(),
		]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some("two"));
		assert_eq!(sliced.get(1), Some("three"));
	}

	#[test]
	fn test_filter() {
		let mut container = Utf8Container::from_vec(vec![
			"keep".to_string(),
			"drop".to_string(),
			"keep".to_string(),
			"drop".to_string(),
		]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some("keep"));
		assert_eq!(container.get(1), Some("keep"));
	}

	#[test]
	fn test_reorder() {
		let mut container =
			Utf8Container::from_vec(vec!["first".to_string(), "second".to_string(), "third".to_string()]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some("third"));
		assert_eq!(container.get(1), Some("first"));
		assert_eq!(container.get(2), Some("second"));
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let mut container = Utf8Container::from_vec(vec!["a".to_string(), "b".to_string()]);
		let indices = [1, 5, 0];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some("b"));
		assert_eq!(container.get(1), Some(""));
		assert_eq!(container.get(2), Some("a"));
	}

	#[test]
	fn test_empty_strings() {
		let mut container = Utf8Container::with_capacity(2);
		container.push("".to_string());
		container.push_default();

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(""));
		assert_eq!(container.get(1), Some(""));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
	}

	#[test]
	fn testault() {
		let container = Utf8Container::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}

	#[test]
	fn test_data_bytes_and_offsets_match_zero_copy_layout() {
		let container = Utf8Container::from_vec(vec!["aa".to_string(), "bb".to_string()]);
		assert_eq!(container.data_bytes(), b"aabb");
		assert_eq!(container.offsets(), &[0u64, 2, 4]);
	}

	#[test]
	fn test_postcard_wire_compat() {
		// The postcard byte form must match what `Vec<String>` would
		// produce so on-disk state and CDC streams stay readable.
		let strings = vec!["hello".to_string(), "world".to_string()];
		let strings_bytes: Vec<u8> = postcard_to_allocvec(&strings).unwrap();

		let container = Utf8Container::from_vec(strings.clone());
		let container_bytes: Vec<u8> = postcard_to_allocvec(&container).unwrap();

		assert_eq!(strings_bytes, container_bytes);
	}
}
