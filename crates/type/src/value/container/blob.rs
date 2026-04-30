// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	storage::{Cow, Storage},
	value::{Value, blob::Blob, container::varlen::VarlenContainer, r#type::Type},
};

pub struct BlobContainer<S: Storage = Cow> {
	inner: VarlenContainer<S>,
}

impl<S: Storage> Clone for BlobContainer<S> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<S: Storage> Debug for BlobContainer<S>
where
	VarlenContainer<S>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BlobContainer").field("inner", &self.inner).finish()
	}
}

impl<S: Storage> PartialEq for BlobContainer<S>
where
	VarlenContainer<S>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.inner == other.inner
	}
}

impl Serialize for BlobContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		// Postcard-stable: the inner VarlenContainer encodes as a sequence
		// of byte slices, matching the previous `Vec<Blob>` (== `Vec<Vec<u8>>`)
		// wire form byte-for-byte.
		self.inner.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for BlobContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		let inner = VarlenContainer::deserialize(deserializer)?;
		Ok(Self {
			inner,
		})
	}
}

impl BlobContainer<Cow> {
	pub fn new(data: Vec<Blob>) -> Self {
		Self::from_vec(data)
	}

	pub fn from_vec(data: Vec<Blob>) -> Self {
		let inner = VarlenContainer::from_byte_slices(data.iter().map(|b| b.as_bytes()));
		Self {
			inner,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		// Heuristic: assume ~32 bytes per blob initially.
		Self {
			inner: VarlenContainer::with_capacity(capacity, capacity * 32),
		}
	}

	/// Build directly from contiguous bytes + offsets.
	pub fn from_bytes_offsets(data: Vec<u8>, offsets: Vec<u64>) -> Self {
		Self {
			inner: VarlenContainer::from_raw_parts(data, offsets),
		}
	}
}

impl<S: Storage> BlobContainer<S> {
	pub fn from_inner(inner: VarlenContainer<S>) -> Self {
		Self {
			inner,
		}
	}

	pub fn from_storage_parts(data: S::Vec<u8>, offsets: S::Vec<u64>) -> Self {
		Self {
			inner: VarlenContainer::from_storage_parts(data, offsets),
		}
	}

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

	pub fn clear(&mut self) {
		self.inner.clear_generic();
	}

	/// Borrow the i-th blob's bytes.
	pub fn get(&self, index: usize) -> Option<&[u8]> {
		self.inner.get_bytes(index)
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
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

	/// Borrow the inner VarlenContainer (test/debug only).
	pub fn inner(&self) -> &VarlenContainer<S> {
		&self.inner
	}

	pub fn as_string(&self, index: usize) -> String {
		match self.get(index) {
			Some(bytes) => Blob::new(bytes.to_vec()).to_string(),
			None => "none".to_string(),
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		match self.get(index) {
			Some(bytes) => Value::Blob(Blob::new(bytes.to_vec())),
			None => Value::none_of(Type::Blob),
		}
	}

	/// Iterate blobs as `Option<&[u8]>`.
	pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> + '_ {
		(0..self.len()).map(|i| self.get(i))
	}

	/// Iterate blobs as `&[u8]` directly.
	pub fn iter_bytes(&self) -> impl Iterator<Item = &[u8]> + '_ {
		(0..self.len()).map(|i| self.get(i).unwrap_or(&[]))
	}
}

impl BlobContainer<Cow> {
	pub fn push(&mut self, value: Blob) {
		self.inner.push_bytes(value.as_bytes());
	}

	pub fn push_bytes(&mut self, value: &[u8]) {
		self.inner.push_bytes(value);
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

impl Default for BlobContainer<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use postcard::to_allocvec as postcard_to_allocvec;

	use super::*;

	#[test]
	fn test_new() {
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![4, 5, 6]);
		let blobs = vec![blob1.clone(), blob2.clone()];
		let container = BlobContainer::new(blobs);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(blob1.as_bytes()));
		assert_eq!(container.get(1), Some(blob2.as_bytes()));
	}

	#[test]
	fn test_from_vec() {
		let blob1 = Blob::new(vec![10, 20, 30]);
		let blob2 = Blob::new(vec![40, 50]);
		let blobs = vec![blob1.clone(), blob2.clone()];
		let container = BlobContainer::from_vec(blobs);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(blob1.as_bytes()));
		assert_eq!(container.get(1), Some(blob2.as_bytes()));

		for i in 0..2 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container = BlobContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_with_default() {
		let mut container = BlobContainer::with_capacity(3);
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![7, 8, 9]);

		container.push(blob1.clone());
		container.push_default();
		container.push(blob2.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(blob1.as_bytes()));
		assert_eq!(container.get(1), Some(b"".as_slice()));
		assert_eq!(container.get(2), Some(blob2.as_bytes()));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn testault() {
		let container = BlobContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}

	#[test]
	fn test_data_bytes_and_offsets_match_zero_copy_layout() {
		let container = BlobContainer::from_vec(vec![Blob::new(vec![0xAA, 0xBB]), Blob::new(vec![0xCC])]);
		assert_eq!(container.data_bytes(), &[0xAAu8, 0xBB, 0xCC]);
		assert_eq!(container.offsets(), &[0u64, 2, 3]);
	}

	#[test]
	fn test_postcard_wire_compat() {
		// The inner VarlenContainer is byte-compatible with `Vec<Vec<u8>>`
		// via postcard. `Blob` derefs to `Vec<u8>`, so a `Vec<Blob>` is
		// also byte-compatible.
		let blobs = vec![Blob::new(vec![1, 2, 3]), Blob::new(vec![4, 5])];
		let blobs_bytes: Vec<u8> = postcard_to_allocvec(&blobs).unwrap();

		let container = BlobContainer::from_vec(blobs.clone());
		let container_bytes: Vec<u8> = postcard_to_allocvec(&container).unwrap();

		assert_eq!(blobs_bytes, container_bytes);
	}
}
