// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	fmt::{self, Debug},
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};
use serde_bytes::{ByteBuf, Bytes};

use crate::{reifydb_assertions, util::shared_vec::SharedVec};

pub struct VarlenContainer {
	data: SharedVec<u8>,
	offsets: SharedVec<u64>,
}

impl Clone for VarlenContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			offsets: self.offsets.clone(),
		}
	}
}

impl Debug for VarlenContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("VarlenContainer")
			.field("len", &self.len())
			.field("data_bytes", &self.data_byte_len())
			.finish()
	}
}

impl PartialEq for VarlenContainer {
	fn eq(&self, other: &Self) -> bool {
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

impl VarlenContainer {
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
			data: SharedVec::from_vec(data),
			offsets: SharedVec::from_vec(offsets),
		}
	}

	pub fn from_repeated_bytes(item: &[u8], count: usize) -> Self {
		let mut data: Vec<u8> = Vec::with_capacity(item.len() * count);
		let mut offsets: Vec<u64> = Vec::with_capacity(count + 1);
		offsets.push(0);
		for _ in 0..count {
			data.extend_from_slice(item);
			offsets.push(data.len() as u64);
		}
		Self {
			data: SharedVec::from_vec(data),
			offsets: SharedVec::from_vec(offsets),
		}
	}

	pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
		let mut offsets = Vec::with_capacity(item_capacity + 1);
		offsets.push(0);
		Self {
			data: SharedVec::with_capacity(data_capacity),
			offsets: SharedVec::from_vec(offsets),
		}
	}

	pub fn empty() -> Self {
		Self {
			data: SharedVec::new(),
			offsets: SharedVec::from_vec(vec![0]),
		}
	}

	pub fn from_raw_parts(data: Vec<u8>, offsets: Vec<u64>) -> Self {
		reifydb_assertions! {
			assert!(!offsets.is_empty(), "offsets must always have offsets[0] = 0");
			assert_eq!(offsets[0], 0, "offsets[0] must be 0");
			assert_eq!(*offsets.last().unwrap() as usize, data.len(), "offsets[len] must equal data.len()");
		}
		Self {
			data: SharedVec::from_vec(data),
			offsets: SharedVec::from_vec(offsets),
		}
	}
}

impl VarlenContainer {
	pub fn len(&self) -> usize {
		self.offsets.len().saturating_sub(1)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn data_byte_len(&self) -> usize {
		let (first, last) = self.bounds();
		(last - first) as usize
	}

	pub fn compact_parts(&self) -> (&[u8], Cow<'_, [u64]>) {
		let (first, last) = self.bounds();
		let data = &self.data[first as usize..last as usize];
		let offsets = if first == 0 {
			Cow::Borrowed(self.offsets.as_slice())
		} else {
			Cow::Owned(self.offsets.iter().map(|offset| offset - first).collect())
		};
		(data, offsets)
	}

	pub fn get_bytes(&self, idx: usize) -> Option<&[u8]> {
		if idx >= self.len() {
			return None;
		}
		let start = self.offsets[idx] as usize;
		let end = self.offsets[idx + 1] as usize;
		self.data.get(start..end)
	}

	pub fn capacity(&self) -> usize {
		self.offsets.capacity().saturating_sub(1)
	}

	pub fn heap_size(&self) -> usize {
		self.data.capacity() + self.offsets.capacity() * size_of::<u64>()
	}

	pub fn freeze(&mut self) {
		self.data.freeze();
		self.offsets.freeze();
	}

	fn bounds(&self) -> (u64, u64) {
		match (self.offsets.first(), self.offsets.last()) {
			(Some(&first), Some(&last)) => (first, last),
			_ => panic!("varlen offsets must hold at least the leading offset"),
		}
	}

	fn make_mut(&mut self) -> (&mut Vec<u8>, &mut Vec<u64>) {
		let (first, last) = self.bounds();
		if first != 0 || last as usize != self.data.len() {
			self.data = SharedVec::from_vec(self.data[first as usize..last as usize].to_vec());
			for offset in self.offsets.make_mut() {
				*offset -= first;
			}
		}
		(self.data.make_mut(), self.offsets.make_mut())
	}
}

impl VarlenContainer {
	pub fn clear(&mut self) {
		self.data.clear();
		self.offsets.clear();
		self.offsets.push(0);
	}

	pub fn push_bytes(&mut self, bytes: &[u8]) {
		let (data, offsets) = self.make_mut();
		data.extend_from_slice(bytes);
		offsets.push(data.len() as u64);
	}

	pub fn extend_from(&mut self, other: &Self) {
		let (other_first, other_last) = other.bounds();
		let other_data = &other.data[other_first as usize..other_last as usize];
		let other_offsets = other.offsets.as_slice();

		let (data, offsets) = self.make_mut();
		let base = data.len() as u64;
		data.extend_from_slice(other_data);
		offsets.extend(other_offsets.iter().skip(1).map(|&o| base + (o - other_first)));
	}

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
		self.data = SharedVec::from_vec(new_data);
		self.offsets = SharedVec::from_vec(new_offsets);
	}

	pub fn reorder_in_place(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(self.data_byte_len());
		let mut new_offsets = Vec::with_capacity(indices.len() + 1);
		new_offsets.push(0);
		for &idx in indices {
			let bytes = self.get_bytes(idx).unwrap_or(&[]);
			new_data.extend_from_slice(bytes);
			new_offsets.push(new_data.len() as u64);
		}
		self.data = SharedVec::from_vec(new_data);
		self.offsets = SharedVec::from_vec(new_offsets);
	}

	pub fn take_n(&self, n: usize) -> Self {
		self.slice(0, n)
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let len = self.len();
		let start = start.min(len);
		let end = end.min(len);
		if start >= end {
			return Self::empty();
		}
		if self.data.is_frozen() && self.offsets.is_frozen() {
			return Self {
				data: self.data.clone(),
				offsets: self.offsets.slice(start, end + 1),
			};
		}
		let start_byte = self.offsets[start];
		let end_byte = self.offsets[end];
		let new_data = self.data[start_byte as usize..end_byte as usize].to_vec();
		let new_offsets = self.offsets[start..=end].iter().map(|offset| offset - start_byte).collect();
		Self::from_raw_parts(new_data, new_offsets)
	}
}

impl Serialize for VarlenContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		let mut seq = serializer.serialize_seq(Some(self.len()))?;
		for i in 0..self.len() {
			let bytes = self.get_bytes(i).unwrap_or(&[]);
			seq.serialize_element(Bytes::new(bytes))?;
		}
		seq.end()
	}
}

impl<'de> Deserialize<'de> for VarlenContainer {
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

impl Default for VarlenContainer {
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
		assert_eq!(&*c.compact_parts().1, &[0u64]);
		assert!(c.compact_parts().0.is_empty());
	}

	#[test]
	fn push_bytes_appends_and_updates_offsets() {
		let mut c = VarlenContainer::empty();
		c.push_bytes(b"hello");
		c.push_bytes(b"");
		c.push_bytes(b"world");
		assert_eq!(c.len(), 3);
		assert_eq!(&*c.compact_parts().1, &[0u64, 5, 5, 10]);
		assert_eq!(c.compact_parts().0, b"helloworld");
		assert_eq!(c.get_bytes(0), Some(b"hello".as_slice()));
		assert_eq!(c.get_bytes(1), Some(b"".as_slice()));
		assert_eq!(c.get_bytes(2), Some(b"world".as_slice()));
		assert_eq!(c.get_bytes(3), None);
	}

	#[test]
	fn from_byte_slices_round_trip() {
		let c = VarlenContainer::from_byte_slices([b"a".as_slice(), b"bc", b"def"]);
		assert_eq!(c.len(), 3);
		assert_eq!(c.compact_parts().0, b"abcdef");
		assert_eq!(&*c.compact_parts().1, &[0u64, 1, 3, 6]);
	}

	#[test]
	fn from_repeated_bytes_matches_explicit_copies() {
		let repeated = VarlenContainer::from_repeated_bytes(b"abc", 3);
		let explicit = VarlenContainer::from_byte_slices([b"abc".as_slice(), b"abc", b"abc"]);
		assert_eq!(repeated, explicit);
		assert_eq!(repeated.len(), 3);
		assert_eq!(repeated.compact_parts().0, b"abcabcabc");
		assert_eq!(&*repeated.compact_parts().1, &[0u64, 3, 6, 9]);
	}

	#[test]
	fn from_repeated_bytes_zero_count_is_empty() {
		let c = VarlenContainer::from_repeated_bytes(b"abc", 0);
		assert_eq!(c.len(), 0);
		assert_eq!(&*c.compact_parts().1, &[0u64]);
		assert!(c.compact_parts().0.is_empty());
	}

	#[test]
	fn from_repeated_bytes_empty_item_keeps_count() {
		let c = VarlenContainer::from_repeated_bytes(b"", 4);
		assert_eq!(c.len(), 4);
		assert_eq!(c.get_bytes(0), Some(b"".as_slice()));
		assert_eq!(c.get_bytes(3), Some(b"".as_slice()));
		assert!(c.compact_parts().0.is_empty());
	}

	#[test]
	fn clear_resets_to_empty_state() {
		let mut c = VarlenContainer::from_byte_slices([b"x".as_slice(), b"y"]);
		c.clear();
		assert_eq!(c.len(), 0);
		assert_eq!(&*c.compact_parts().1, &[0u64]);
		assert!(c.compact_parts().0.is_empty());
	}

	#[test]
	fn extend_from_concatenates_and_rebases_offsets() {
		let mut a = VarlenContainer::from_byte_slices([b"foo".as_slice()]);
		let b = VarlenContainer::from_byte_slices([b"bar".as_slice(), b"baz"]);
		a.extend_from(&b);
		assert_eq!(a.len(), 3);
		assert_eq!(a.compact_parts().0, b"foobarbaz");
		assert_eq!(&*a.compact_parts().1, &[0u64, 3, 6, 9]);
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
		assert_eq!(&*s.compact_parts().1, &[0u64, 2, 4]);
	}

	#[test]
	fn serde_round_trip_preserves_content() {
		let original = VarlenContainer::from_byte_slices([b"hello".as_slice(), b"", b"world"]);
		let encoded: Vec<u8> = postcard_to_allocvec(&original).unwrap();
		let decoded: VarlenContainer = postcard_from_bytes(&encoded).unwrap();
		assert_eq!(decoded.len(), 3);
		assert_eq!(decoded.get_bytes(0), Some(b"hello".as_slice()));
		assert_eq!(decoded.get_bytes(1), Some(b"".as_slice()));
		assert_eq!(decoded.get_bytes(2), Some(b"world".as_slice()));
	}

	#[test]
	fn serde_wire_compat_with_vec_of_strings() {
		// Postcard encodes `Vec<String>` and `Vec<&[u8]>` identically (both are length-prefixed
		// sequences of length-prefixed bytes), so the two are wire-compatible.
		let strings = vec!["a".to_string(), "bc".to_string(), "def".to_string()];
		let encoded: Vec<u8> = postcard_to_allocvec(&strings).unwrap();
		let decoded: VarlenContainer = postcard_from_bytes(&encoded).unwrap();
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

	fn frozen_abcd() -> VarlenContainer {
		let mut c = VarlenContainer::from_byte_slices([b"aa".as_slice(), b"bb", b"cc", b"dd"]);
		c.freeze();
		c
	}

	#[test]
	fn frozen_slice_shares_both_buffers() {
		// A slice that copies instead of sharing would leave both handles unshared.
		let c = frozen_abcd();
		let s = c.slice(1, 3);
		assert_eq!(s.data.as_ptr(), c.data.as_ptr());
		assert_eq!(s.offsets.as_ptr(), c.offsets[1..].as_ptr());
		assert_eq!(s.len(), 2);
		assert_eq!(s.get_bytes(0), Some(b"bb".as_slice()));
		assert_eq!(s.get_bytes(1), Some(b"cc".as_slice()));
		assert_eq!(s.get_bytes(2), None);
		assert_eq!(s.data_byte_len(), 4);
	}

	#[test]
	fn frozen_slice_compact_parts_are_rebased() {
		// A guest must see only the referenced bytes and a first offset of 0, never the parent buffer.
		let c = frozen_abcd();
		let s = c.slice(1, 3);
		let (data, offsets) = s.compact_parts();
		assert_eq!(data, b"bbcc");
		assert_eq!(&*offsets, &[0u64, 2, 4]);
		assert!(matches!(offsets, Cow::Owned(_)));
	}

	#[test]
	fn unsliced_compact_parts_borrow_offsets() {
		// Unsliced containers must stay zero-copy across the ABI.
		let c = frozen_abcd();
		let (data, offsets) = c.compact_parts();
		assert_eq!(data, b"aabbccdd");
		assert_eq!(&*offsets, &[0u64, 2, 4, 6, 8]);
		assert!(matches!(offsets, Cow::Borrowed(_)));
	}

	#[test]
	fn push_after_frozen_slice_does_not_expose_parent_bytes() {
		// Appending at the shared buffer's end would make the new row start with the parent's "dd".
		let c = frozen_abcd();
		let mut s = c.slice(1, 3);
		s.push_bytes(b"xy");
		assert_eq!(s.len(), 3);
		assert_eq!(s.get_bytes(2), Some(b"xy".as_slice()));
		let (data, offsets) = s.compact_parts();
		assert_eq!(data, b"bbccxy");
		assert_eq!(&*offsets, &[0u64, 2, 4, 6]);
		assert_eq!(c.get_bytes(3), Some(b"dd".as_slice()));
		assert_eq!(c.len(), 4);
	}

	#[test]
	fn extend_from_frozen_slice_rebases_other_offsets() {
		// Copying the other side's absolute offsets unrebased would point past the appended bytes.
		let mut a = VarlenContainer::from_byte_slices([b"foo".as_slice()]);
		let s = frozen_abcd().slice(2, 4);
		a.extend_from(&s);
		assert_eq!(a.len(), 3);
		assert_eq!(a.get_bytes(1), Some(b"cc".as_slice()));
		assert_eq!(a.get_bytes(2), Some(b"dd".as_slice()));
		let (data, offsets) = a.compact_parts();
		assert_eq!(data, b"fooccdd");
		assert_eq!(&*offsets, &[0u64, 3, 5, 7]);
	}

	#[test]
	fn take_n_of_frozen_container_shares_buffers() {
		// take_n must be an O(1) view once frozen, not a copy.
		let c = frozen_abcd();
		let t = c.take_n(3);
		assert_eq!(t.data.as_ptr(), c.data.as_ptr());
		assert_eq!(t.offsets.as_ptr(), c.offsets.as_ptr());
		assert_eq!(t.len(), 3);
		assert_eq!(t.get_bytes(2), Some(b"cc".as_slice()));
		assert_eq!(t.data_byte_len(), 6);
	}

	#[test]
	fn empty_range_slice_of_frozen_container_is_empty() {
		// An empty range must yield len 0 with no referenced bytes, even when the clamp lands mid-buffer.
		let c = frozen_abcd();
		for (start, end) in [(2, 2), (3, 1), (10, 20)] {
			let s = c.slice(start, end);
			assert_eq!(s.len(), 0);
			assert_eq!(s.data_byte_len(), 0);
			assert_eq!(s.compact_parts().0, b"");
		}
	}

	#[test]
	fn filter_on_frozen_slice_keeps_right_rows() {
		// filter must read rows through absolute offsets, not assume the buffer starts at the slice.
		let mut s = frozen_abcd().slice(1, 4);
		s.filter_in_place(|i| i != 1);
		assert_eq!(s.len(), 2);
		assert_eq!(s.get_bytes(0), Some(b"bb".as_slice()));
		assert_eq!(s.get_bytes(1), Some(b"dd".as_slice()));
		assert_eq!(s.data.as_slice(), b"bbdd");
		assert_eq!(s.offsets.as_slice(), &[0u64, 2, 4]);
	}

	#[test]
	fn clear_on_frozen_slice_leaves_parent_intact() {
		// Clearing a shared handle must never clear the buffer other handles still read.
		let c = frozen_abcd();
		let mut s = c.slice(0, 2);
		s.clear();
		assert_eq!(s.len(), 0);
		assert_eq!(c.len(), 4);
		assert_eq!(c.get_bytes(0), Some(b"aa".as_slice()));
	}
}
