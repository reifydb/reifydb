// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared storage primitive for variable-length column types (Utf8, Blob).
//!
//! Layout: a contiguous byte payload `data` plus an `offsets` array of length
//! `len + 1` such that the i-th element occupies `data[offsets[i]..offsets[i+1]]`.
//! `offsets[0]` is always 0, `offsets[len]` is always `data.len()`.
//!
//! This layout matches the FFI wire format (`ColumnDataFFI`) byte-for-byte,
//! so the marshal path can hand guests a `cap == 0` borrow of the host's
//! native storage with no transformation.

use std::{
	fmt::{self, Debug},
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};
use serde_bytes::{ByteBuf, Bytes};

use crate::{
	storage::{Cow, DataVec, Storage},
	util::cowvec::CowVec,
};

// We rely on `DataVec` for storage-generic operations.
// CowVec-specific paths use `Cow` storage explicitly.

pub struct VarlenContainer<S: Storage = Cow> {
	data: S::Vec<u8>,
	offsets: S::Vec<u64>,
}

impl<S: Storage> Clone for VarlenContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			offsets: self.offsets.clone(),
		}
	}
}

impl<S: Storage> Debug for VarlenContainer<S>
where
	S::Vec<u8>: Debug,
	S::Vec<u64>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("VarlenContainer")
			.field("len", &self.len())
			.field("data_bytes", &self.data.len())
			.finish()
	}
}

impl<S: Storage> PartialEq for VarlenContainer<S>
where
	S::Vec<u8>: PartialEq,
	S::Vec<u64>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		// Compare logical contents, not byte-identical layout
		if self.len() != other.len() {
			return false;
		}
		for i in 0..self.len() {
			if self.get_bytes(i) != other.get_bytes(i) {
				return false;
			}
		}
		true
	}
}

impl VarlenContainer<Cow> {
	/// Build a fresh container from a sequence of `&[u8]` slices.
	pub fn from_byte_slices<'a, I>(items: I) -> Self
	where
		I: IntoIterator<Item = &'a [u8]>,
	{
		let mut offsets: Vec<u64> = vec![0];
		let mut data: Vec<u8> = Vec::new();
		for item in items {
			data.extend_from_slice(item);
			offsets.push(data.len() as u64);
		}
		Self {
			data: CowVec::new(data),
			offsets: CowVec::new(offsets),
		}
	}

	/// Pre-allocate space for `len` items and `data_bytes` bytes of payload.
	pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
		let mut offsets = Vec::with_capacity(item_capacity + 1);
		offsets.push(0);
		Self {
			data: CowVec::new(Vec::with_capacity(data_capacity)),
			offsets: CowVec::new(offsets),
		}
	}

	/// Empty container with zero capacity.
	pub fn empty() -> Self {
		Self {
			data: CowVec::new(Vec::new()),
			offsets: CowVec::new(vec![0]),
		}
	}

	pub fn from_raw_parts(data: Vec<u8>, offsets: Vec<u64>) -> Self {
		debug_assert!(!offsets.is_empty(), "offsets must always have offsets[0] = 0");
		debug_assert_eq!(offsets[0], 0, "offsets[0] must be 0");
		debug_assert_eq!(*offsets.last().unwrap() as usize, data.len(), "offsets[len] must equal data.len()");
		Self {
			data: CowVec::new(data),
			offsets: CowVec::new(offsets),
		}
	}
}

impl<S: Storage> VarlenContainer<S> {
	/// Construct directly from storage-generic `S::Vec<u8>` + `S::Vec<u64>`.
	/// Used by arena-conversion code that builds Bump-backed containers
	/// from already-allocated parts.
	pub fn from_storage_parts(data: S::Vec<u8>, offsets: S::Vec<u64>) -> Self {
		debug_assert!(
			DataVec::len(&offsets) >= 1,
			"offsets must always include the leading 0; got empty offsets"
		);
		Self {
			data,
			offsets,
		}
	}

	pub fn len(&self) -> usize {
		// offsets always has at least one element (the leading 0)
		DataVec::len(&self.offsets).saturating_sub(1)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn data_byte_len(&self) -> usize {
		DataVec::len(&self.data)
	}

	/// Borrow the concatenated payload bytes.
	pub fn data_bytes(&self) -> &[u8] {
		self.data.as_slice()
	}

	/// Borrow the offsets array. Length is `len + 1`.
	pub fn offsets(&self) -> &[u64] {
		self.offsets.as_slice()
	}

	pub fn get_bytes(&self, idx: usize) -> Option<&[u8]> {
		if idx >= self.len() {
			return None;
		}
		let start = self.offsets.as_slice()[idx] as usize;
		let end = self.offsets.as_slice()[idx + 1] as usize;
		self.data.as_slice().get(start..end)
	}

	pub fn capacity(&self) -> usize {
		// Number of items the offsets vec can hold without realloc - 1 for the leading zero
		DataVec::capacity(&self.offsets).saturating_sub(1)
	}

	pub fn data(&self) -> &S::Vec<u8> {
		&self.data
	}

	pub fn offsets_data(&self) -> &S::Vec<u64> {
		&self.offsets
	}

	/// Storage-generic clear; storage-generic implementations call this.
	pub fn clear_generic(&mut self) {
		DataVec::clear(&mut self.data);
		DataVec::clear(&mut self.offsets);
		DataVec::push(&mut self.offsets, 0u64);
	}
}

impl VarlenContainer<Cow> {
	pub fn clear(&mut self) {
		let data_mut = self.data.make_mut();
		data_mut.clear();
		let offsets_mut = self.offsets.make_mut();
		offsets_mut.clear();
		offsets_mut.push(0);
	}

	/// Append a byte slice as a new element.
	pub fn push_bytes(&mut self, bytes: &[u8]) {
		let data_mut = self.data.make_mut();
		data_mut.extend_from_slice(bytes);
		let new_end = data_mut.len() as u64;
		self.offsets.make_mut().push(new_end);
	}

	/// Extend with all elements from another container.
	pub fn extend_from(&mut self, other: &Self) {
		// Extending offsets: each new offset = base + (other_offset[i] - other_offset[0])
		let base = DataVec::len(&self.data) as u64;
		let other_offsets = other.offsets.as_slice();
		let other_data = other.data.as_slice();

		let data_mut = self.data.make_mut();
		data_mut.extend_from_slice(other_data);

		let offsets_mut = self.offsets.make_mut();
		// Skip the leading 0 in `other_offsets` since we already have our own.
		offsets_mut.extend(other_offsets.iter().skip(1).map(|&o| base + o));
	}

	/// Filter in place via a callback that decides whether to keep each
	/// element. Builds a new (data, offsets) buffer in one pass.
	pub fn filter_in_place<F: FnMut(usize) -> bool>(&mut self, mut keep: F) {
		let len = self.len();
		let mut new_data = Vec::with_capacity(self.data_byte_len());
		let mut new_offsets = Vec::with_capacity(len + 1);
		new_offsets.push(0);
		for i in 0..len {
			if keep(i) {
				let bytes = self.get_bytes(i).unwrap_or(&[]);
				new_data.extend_from_slice(bytes);
				new_offsets.push(new_data.len() as u64);
			}
		}
		self.data = CowVec::new(new_data);
		self.offsets = CowVec::new(new_offsets);
	}

	/// Reorder according to indices; out-of-bounds indices yield empty
	/// elements.
	pub fn reorder_in_place(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(self.data_byte_len());
		let mut new_offsets = Vec::with_capacity(indices.len() + 1);
		new_offsets.push(0);
		for &idx in indices {
			let bytes = self.get_bytes(idx).unwrap_or(&[]);
			new_data.extend_from_slice(bytes);
			new_offsets.push(new_data.len() as u64);
		}
		self.data = CowVec::new(new_data);
		self.offsets = CowVec::new(new_offsets);
	}

	/// Take the first `n` elements as a new container.
	pub fn take_n(&self, n: usize) -> Self {
		let n = n.min(self.len());
		let end_byte = self.offsets.as_slice()[n] as usize;
		let new_data: Vec<u8> = self.data.as_slice()[..end_byte].to_vec();
		let new_offsets: Vec<u64> = self.offsets.as_slice()[..=n].to_vec();
		Self::from_raw_parts(new_data, new_offsets)
	}

	/// Slice elements `[start, end)` as a new container.
	pub fn slice(&self, start: usize, end: usize) -> Self {
		let len = self.len();
		let start = start.min(len);
		let end = end.min(len);
		if start >= end {
			return Self::empty();
		}
		let start_byte = self.offsets.as_slice()[start] as usize;
		let end_byte = self.offsets.as_slice()[end] as usize;
		let new_data: Vec<u8> = self.data.as_slice()[start_byte..end_byte].to_vec();
		// Re-base offsets so the first one is 0.
		let new_offsets: Vec<u64> =
			self.offsets.as_slice()[start..=end].iter().map(|o| *o - start_byte as u64).collect();
		Self::from_raw_parts(new_data, new_offsets)
	}
}

// Postcard-stable serde: encode as a sequence of `&[u8]` so the byte stream
// matches the existing `Vec<Vec<u8>>` / `Vec<String>` encoding. Deserialize
// via a temporary Vec<Vec<u8>> and rebuild the contiguous layout.
impl Serialize for VarlenContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		let mut seq = serializer.serialize_seq(Some(self.len()))?;
		for i in 0..self.len() {
			let bytes = self.get_bytes(i).unwrap_or(&[]);
			seq.serialize_element(Bytes::new(bytes))?;
		}
		seq.end()
	}
}

impl<'de> Deserialize<'de> for VarlenContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		let items: Vec<ByteBuf> = Vec::deserialize(deserializer)?;
		let total: usize = items.iter().map(|b| b.len()).sum();
		let mut data = Vec::with_capacity(total);
		let mut offsets = Vec::with_capacity(items.len() + 1);
		offsets.push(0);
		for item in items {
			data.extend_from_slice(item.as_slice());
			offsets.push(data.len() as u64);
		}
		Ok(Self::from_raw_parts(data, offsets))
	}
}

impl Default for VarlenContainer<Cow> {
	fn default() -> Self {
		Self::empty()
	}
}

#[cfg(test)]
mod tests {
	use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};

	use super::*;

	#[test]
	fn empty_has_zero_len_and_offsets_with_one_zero() {
		let c = VarlenContainer::empty();
		assert_eq!(c.len(), 0);
		assert_eq!(c.offsets(), &[0u64]);
		assert!(c.data_bytes().is_empty());
	}

	#[test]
	fn push_bytes_appends_and_updates_offsets() {
		let mut c = VarlenContainer::empty();
		c.push_bytes(b"hello");
		c.push_bytes(b"");
		c.push_bytes(b"world");
		assert_eq!(c.len(), 3);
		assert_eq!(c.offsets(), &[0u64, 5, 5, 10]);
		assert_eq!(c.data_bytes(), b"helloworld");
		assert_eq!(c.get_bytes(0), Some(b"hello".as_slice()));
		assert_eq!(c.get_bytes(1), Some(b"".as_slice()));
		assert_eq!(c.get_bytes(2), Some(b"world".as_slice()));
		assert_eq!(c.get_bytes(3), None);
	}

	#[test]
	fn from_byte_slices_round_trip() {
		let c = VarlenContainer::from_byte_slices([b"a".as_slice(), b"bc", b"def"]);
		assert_eq!(c.len(), 3);
		assert_eq!(c.data_bytes(), b"abcdef");
		assert_eq!(c.offsets(), &[0u64, 1, 3, 6]);
	}

	#[test]
	fn clear_resets_to_empty_state() {
		let mut c = VarlenContainer::from_byte_slices([b"x".as_slice(), b"y"]);
		c.clear();
		assert_eq!(c.len(), 0);
		assert_eq!(c.offsets(), &[0u64]);
		assert!(c.data_bytes().is_empty());
	}

	#[test]
	fn extend_from_concatenates_and_rebases_offsets() {
		let mut a = VarlenContainer::from_byte_slices([b"foo".as_slice()]);
		let b = VarlenContainer::from_byte_slices([b"bar".as_slice(), b"baz"]);
		a.extend_from(&b);
		assert_eq!(a.len(), 3);
		assert_eq!(a.data_bytes(), b"foobarbaz");
		assert_eq!(a.offsets(), &[0u64, 3, 6, 9]);
	}

	#[test]
	fn filter_in_place_keeps_matching_elements() {
		let mut c = VarlenContainer::from_byte_slices([b"yes".as_slice(), b"no", b"yes", b"no"]);
		c.filter_in_place(|i| i % 2 == 0);
		assert_eq!(c.len(), 2);
		assert_eq!(c.get_bytes(0), Some(b"yes".as_slice()));
		assert_eq!(c.get_bytes(1), Some(b"yes".as_slice()));
	}

	#[test]
	fn reorder_in_place_handles_oob_as_empty() {
		let mut c = VarlenContainer::from_byte_slices([b"a".as_slice(), b"b"]);
		c.reorder_in_place(&[1, 100, 0]);
		assert_eq!(c.len(), 3);
		assert_eq!(c.get_bytes(0), Some(b"b".as_slice()));
		assert_eq!(c.get_bytes(1), Some(b"".as_slice()));
		assert_eq!(c.get_bytes(2), Some(b"a".as_slice()));
	}

	#[test]
	fn take_n_truncates() {
		let c = VarlenContainer::from_byte_slices([b"a".as_slice(), b"b", b"c"]);
		let t = c.take_n(2);
		assert_eq!(t.len(), 2);
		assert_eq!(t.get_bytes(0), Some(b"a".as_slice()));
		assert_eq!(t.get_bytes(1), Some(b"b".as_slice()));
	}

	#[test]
	fn slice_extracts_subrange_with_rebased_offsets() {
		let c = VarlenContainer::from_byte_slices([b"aa".as_slice(), b"bb", b"cc", b"dd"]);
		let s = c.slice(1, 3);
		assert_eq!(s.len(), 2);
		assert_eq!(s.get_bytes(0), Some(b"bb".as_slice()));
		assert_eq!(s.get_bytes(1), Some(b"cc".as_slice()));
		assert_eq!(s.offsets(), &[0u64, 2, 4]);
	}

	#[test]
	fn serde_round_trip_preserves_content() {
		let original = VarlenContainer::from_byte_slices([b"hello".as_slice(), b"", b"world"]);
		let encoded: Vec<u8> = postcard_to_allocvec(&original).unwrap();
		let decoded: VarlenContainer<Cow> = postcard_from_bytes(&encoded).unwrap();
		assert_eq!(decoded.len(), 3);
		assert_eq!(decoded.get_bytes(0), Some(b"hello".as_slice()));
		assert_eq!(decoded.get_bytes(1), Some(b"".as_slice()));
		assert_eq!(decoded.get_bytes(2), Some(b"world".as_slice()));
	}

	#[test]
	fn serde_wire_compat_with_vec_of_strings() {
		// `Vec<&[u8]>` postcard form == `Vec<String>` postcard form when
		// the bytes are valid UTF-8: both are length-prefixed sequences
		// of length-prefixed bytes. Verify by encoding a Vec<String> and
		// decoding into VarlenContainer.
		let strings = vec!["a".to_string(), "bc".to_string(), "def".to_string()];
		let encoded: Vec<u8> = postcard_to_allocvec(&strings).unwrap();
		let decoded: VarlenContainer<Cow> = postcard_from_bytes(&encoded).unwrap();
		assert_eq!(decoded.len(), 3);
		assert_eq!(decoded.get_bytes(0), Some(b"a".as_slice()));
		assert_eq!(decoded.get_bytes(1), Some(b"bc".as_slice()));
		assert_eq!(decoded.get_bytes(2), Some(b"def".as_slice()));
	}

	#[test]
	fn equality_compares_logical_content() {
		let a = VarlenContainer::from_byte_slices([b"x".as_slice(), b"y"]);
		let b = VarlenContainer::from_byte_slices([b"x".as_slice(), b"y"]);
		assert_eq!(a, b);
	}
}
