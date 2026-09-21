// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, fmt, sync::Arc};

use arrow_buffer::{
	bit_chunk_iterator::{BitChunks, UnalignedBitChunk},
	bit_mask::set_bits,
	bit_util::{get_bit, set_bit, unset_bit},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub struct BitVec {
	inner: Arc<BitVecInner>,
	offset: usize,
	len: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct BitVecInner {
	bits: Vec<u8>,
	len: usize,
}

impl Clone for BitVec {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
			offset: self.offset,
			len: self.len,
		}
	}
}

impl Default for BitVec {
	fn default() -> Self {
		Self::empty()
	}
}

impl From<&BitVec> for BitVec {
	fn from(value: &BitVec) -> Self {
		value.clone()
	}
}

impl From<Vec<bool>> for BitVec {
	fn from(value: Vec<bool>) -> Self {
		BitVec::from_slice(&value)
	}
}

impl<const N: usize> From<[bool; N]> for BitVec {
	fn from(value: [bool; N]) -> Self {
		BitVec::from_slice(&value)
	}
}

pub struct BitVecIter {
	inner: Arc<BitVecInner>,
	pos: usize,
	end: usize,
}

impl Iterator for BitVecIter {
	type Item = bool;

	fn next(&mut self) -> Option<Self::Item> {
		if self.pos >= self.end {
			return None;
		}
		let bit = get_bit(&self.inner.bits, self.pos);
		self.pos += 1;
		Some(bit)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.end - self.pos;
		(remaining, Some(remaining))
	}
}

impl ExactSizeIterator for BitVecIter {}

fn clear_trailing(bits: &mut [u8], len: usize) {
	let used = len % 8;
	if used != 0
		&& let Some(last) = bits.get_mut(len / 8)
	{
		*last &= (1u8 << used) - 1;
	}
}

impl BitVec {
	fn from_inner(bits: Vec<u8>, len: usize) -> Self {
		Self {
			inner: Arc::new(BitVecInner {
				bits,
				len,
			}),
			offset: 0,
			len,
		}
	}

	fn is_whole(&self) -> bool {
		self.offset == 0 && self.len == self.inner.len
	}

	pub fn repeat(len: usize, value: bool) -> Self {
		let fill = if value {
			0xFF
		} else {
			0x00
		};
		let mut bits = vec![fill; len.div_ceil(8)];
		clear_trailing(&mut bits, len);
		Self::from_inner(bits, len)
	}

	pub fn from_slice(slice: &[bool]) -> Self {
		Self::from_fn(slice.len(), |i| slice[i])
	}

	pub fn empty() -> Self {
		Self::from_inner(Vec::new(), 0)
	}

	pub fn from_fn(len: usize, mut f: impl FnMut(usize) -> bool) -> Self {
		let mut bits = vec![0u8; len.div_ceil(8)];
		for i in 0..len {
			if f(i) {
				set_bit(&mut bits, i);
			}
		}
		Self::from_inner(bits, len)
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self::from_inner(Vec::with_capacity(capacity.div_ceil(8)), 0)
	}

	pub fn from_raw(bits: Vec<u8>, len: usize) -> Self {
		Self::from_inner(bits, len)
	}

	pub fn slice(&self, start: usize, end: usize) -> BitVec {
		let end = end.min(self.len);
		let start = start.min(end);
		Self {
			inner: Arc::clone(&self.inner),
			offset: self.offset + start,
			len: end - start,
		}
	}

	pub fn take(&self, n: usize) -> BitVec {
		self.slice(0, n)
	}

	fn packed_copy(&self) -> Vec<u8> {
		let mut bits = vec![0u8; self.len.div_ceil(8)];
		set_bits(&mut bits, &self.inner.bits, 0, self.offset, self.len);
		bits
	}

	fn normalize(&mut self) {
		let len = self.len;
		if self.offset == 0
			&& let Some(inner) = Arc::get_mut(&mut self.inner)
		{
			inner.bits.truncate(len.div_ceil(8));
			clear_trailing(&mut inner.bits, len);
			inner.len = len;
			return;
		}
		self.inner = Arc::new(BitVecInner {
			bits: self.packed_copy(),
			len,
		});
		self.offset = 0;
	}

	fn make_mut(&mut self) -> &mut BitVecInner {
		if !self.is_whole() {
			self.normalize();
		}
		Arc::make_mut(&mut self.inner)
	}

	pub fn extend(&mut self, other: &BitVec) {
		let start = self.len;
		let total = start + other.len;
		let inner = self.make_mut();
		inner.bits.resize(total.div_ceil(8), 0);
		set_bits(&mut inner.bits, &other.inner.bits, start, other.offset, other.len);
		inner.len = total;
		self.len = total;
	}

	pub fn clear(&mut self) {
		match Arc::get_mut(&mut self.inner) {
			Some(inner) => {
				inner.bits.clear();
				inner.len = 0;
			}
			None => {
				self.inner = Arc::new(BitVecInner {
					bits: Vec::new(),
					len: 0,
				})
			}
		}
		self.offset = 0;
		self.len = 0;
	}

	pub fn push(&mut self, bit: bool) {
		let len = self.len;
		let inner = self.make_mut();
		if len / 8 >= inner.bits.len() {
			inner.bits.push(0);
		}
		if bit {
			set_bit(&mut inner.bits, len);
		} else {
			unset_bit(&mut inner.bits, len);
		}
		inner.len = len + 1;
		self.len = len + 1;
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn capacity(&self) -> usize {
		self.inner.bits.capacity() * 8
	}

	pub fn to_packed_bytes(&self) -> Cow<'_, [u8]> {
		if self.offset == 0 {
			Cow::Borrowed(&self.inner.bits[..self.len.div_ceil(8)])
		} else {
			Cow::Owned(self.packed_copy())
		}
	}

	pub fn get(&self, idx: usize) -> bool {
		assert!(idx < self.len);
		get_bit(&self.inner.bits, self.offset + idx)
	}

	pub fn set(&mut self, idx: usize, value: bool) {
		assert!(idx < self.len);
		let inner = self.make_mut();
		if value {
			set_bit(&mut inner.bits, idx);
		} else {
			unset_bit(&mut inner.bits, idx);
		}
	}

	pub fn iter(&self) -> BitVecIter {
		BitVecIter {
			inner: Arc::clone(&self.inner),
			pos: self.offset,
			end: self.offset + self.len,
		}
	}

	fn chunks(&self) -> BitChunks<'_> {
		BitChunks::new(&self.inner.bits, self.offset, self.len)
	}

	fn from_chunks(len: usize, chunks: impl Iterator<Item = u64>, remainder: Option<u64>) -> Self {
		let mut bits = Vec::with_capacity(len.div_ceil(64) * 8);
		for chunk in chunks {
			bits.extend_from_slice(&chunk.to_le_bytes());
		}
		if let Some(remainder) = remainder {
			bits.extend_from_slice(&remainder.to_le_bytes());
		}
		bits.truncate(len.div_ceil(8));
		clear_trailing(&mut bits, len);
		Self::from_inner(bits, len)
	}

	fn zip_with(&self, other: &Self, op: impl Fn(u64, u64) -> u64) -> Self {
		assert_eq!(self.len(), other.len());
		let left = self.chunks();
		let right = other.chunks();
		let remainder = (left.remainder_len() > 0).then(|| op(left.remainder_bits(), right.remainder_bits()));
		Self::from_chunks(self.len, left.iter().zip(right.iter()).map(|(a, b)| op(a, b)), remainder)
	}

	pub fn and(&self, other: &Self) -> Self {
		self.zip_with(other, |a, b| a & b)
	}

	pub fn or(&self, other: &Self) -> Self {
		self.zip_with(other, |a, b| a | b)
	}

	pub fn not(&self) -> Self {
		let chunks = self.chunks();
		let remainder = (chunks.remainder_len() > 0).then(|| !chunks.remainder_bits());
		Self::from_chunks(self.len, chunks.iter().map(|a| !a), remainder)
	}

	pub fn to_vec(&self) -> Vec<bool> {
		self.iter().collect()
	}

	pub fn count_ones(&self) -> usize {
		UnalignedBitChunk::new(&self.inner.bits, self.offset, self.len).count_ones()
	}

	pub fn all_ones(&self) -> bool {
		self.count_ones() == self.len
	}

	pub fn count_zeros(&self) -> usize {
		self.len - self.count_ones()
	}

	pub fn any(&self) -> bool {
		let chunks = self.chunks();
		chunks.iter().any(|chunk| chunk != 0) || chunks.remainder_bits() != 0
	}

	pub fn none(&self) -> bool {
		!self.any()
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		assert_eq!(self.len(), indices.len());
		let len = self.len;
		let mut bits = vec![0u8; len.div_ceil(8)];
		for (new_idx, &old_idx) in indices.iter().enumerate() {
			if self.get(old_idx) {
				set_bit(&mut bits, new_idx);
			}
		}
		*self = Self::from_inner(bits, len);
	}
}

impl PartialEq for BitVec {
	fn eq(&self, other: &Self) -> bool {
		if self.len != other.len {
			return false;
		}
		let left = self.chunks();
		let right = other.chunks();
		left.iter().eq(right.iter()) && left.remainder_bits() == right.remainder_bits()
	}
}

impl fmt::Debug for BitVec {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let inner = if self.is_whole() {
			Cow::Borrowed(&*self.inner)
		} else {
			Cow::Owned(BitVecInner {
				bits: self.packed_copy(),
				len: self.len,
			})
		};
		f.debug_struct("BitVec").field("inner", &inner).finish()
	}
}

impl fmt::Display for BitVec {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for bit in self.iter() {
			write!(
				f,
				"{}",
				if bit {
					'1'
				} else {
					'0'
				}
			)?;
		}
		Ok(())
	}
}

impl Serialize for BitVec {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if self.is_whole() {
			self.inner.serialize(serializer)
		} else {
			BitVecInner {
				bits: self.packed_copy(),
				len: self.len,
			}
			.serialize(serializer)
		}
	}
}

impl<'de> Deserialize<'de> for BitVec {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let inner = BitVecInner::deserialize(deserializer)?;
		Ok(Self::from_inner(inner.bits, inner.len))
	}
}

#[cfg(test)]
pub mod tests {
	mod new {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_all_false() {
			let bv = BitVec::repeat(10, false);
			assert_eq!(bv.len(), 10);
			for i in 0..10 {
				assert!(!bv.get(i), "expected bit {} to be false", i);
			}
		}

		#[test]
		fn test_all_true() {
			let bv = BitVec::repeat(10, true);
			assert_eq!(bv.len(), 10);
			for i in 0..10 {
				assert!(bv.get(i), "expected bit {} to be true", i);
			}
		}
	}

	mod get_and_set {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_ok() {
			let mut bv = BitVec::repeat(16, false);
			bv.set(3, true);
			bv.set(7, true);
			bv.set(15, true);

			assert!(bv.get(3));
			assert!(bv.get(7));
			assert!(bv.get(15));
			assert!(!bv.get(0));
			assert!(!bv.get(14));
		}

		#[test]
		#[should_panic(expected = "assertion failed")]
		fn test_get_out_of_bounds() {
			let bv = BitVec::repeat(8, false);
			bv.get(8);
		}

		#[test]
		#[should_panic(expected = "assertion failed")]
		fn test_set_out_of_bounds() {
			let mut bv = BitVec::repeat(8, false);
			bv.set(8, true);
		}
	}

	mod from_fn {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_ok() {
			let bv = BitVec::from_fn(10, |i| i % 2 == 0);
			for i in 0..10 {
				assert_eq!(bv.get(i), i % 2 == 0, "bit {} mismatch", i);
			}
		}
	}

	mod iter {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_ok() {
			let bv = BitVec::from_fn(4, |i| i % 2 == 0);
			let collected: Vec<bool> = bv.iter().collect();
			assert_eq!(collected, vec![true, false, true, false]);
		}

		#[test]
		fn test_empty() {
			let bv = BitVec::from_fn(0, |i| i % 2 == 0);
			let collected: Vec<bool> = bv.iter().collect();
			assert_eq!(collected, Vec::<bool>::new());
		}
	}

	mod and {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_ok() {
			let a = BitVec::from_fn(8, |i| i % 2 == 0); // 10101010
			let b = BitVec::from_fn(8, |i| i < 4); // 11110000
			let result = a.and(&b); // 10100000
			let expected = [true, false, true, false, false, false, false, false];
			for i in 0..8 {
				assert_eq!(result.get(i), expected[i], "mismatch at bit {}", i);
			}
		}
	}

	mod from_slice {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_empty_slice() {
			let bv = BitVec::from_slice(&[]);
			assert_eq!(bv.len(), 0);
		}

		#[test]
		fn test_single_bit() {
			let bv = BitVec::from_slice(&[true]);
			assert_eq!(bv.len(), 1);
			assert!(bv.get(0));

			let bv = BitVec::from_slice(&[false]);
			assert_eq!(bv.len(), 1);
			assert!(!bv.get(0));
		}

		#[test]
		fn test_multiple_bits() {
			let bv = BitVec::from_slice(&[true, false, true, false, true]);
			assert_eq!(bv.len(), 5);
			assert!(bv.get(0));
			assert!(!bv.get(1));
			assert!(bv.get(2));
			assert!(!bv.get(3));
			assert!(bv.get(4));
		}

		#[test]
		fn test_cross_byte_boundary() {
			let input = [true, false, true, false, true, false, true, false, true];
			let bv = BitVec::from_slice(&input);
			assert_eq!(bv.len(), 9);
			for i in 0..9 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_large_slice() {
			let input: Vec<bool> = (0..1000).map(|i| i % 3 == 0).collect();
			let bv = BitVec::from_slice(&input);
			assert_eq!(bv.len(), 1000);
			for i in 0..1000 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}
	}

	mod from_array {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_from_array_1() {
			let bv = BitVec::from([true]);
			assert_eq!(bv.len(), 1);
			assert!(bv.get(0));
		}

		#[test]
		fn test_from_array_2() {
			let bv = BitVec::from([true, false]);
			assert_eq!(bv.len(), 2);
			assert!(bv.get(0));
			assert!(!bv.get(1));
		}

		#[test]
		fn test_from_array_4() {
			let bv = BitVec::from([true, false, true, false]);
			assert_eq!(bv.len(), 4);
			assert!(bv.get(0));
			assert!(!bv.get(1));
			assert!(bv.get(2));
			assert!(!bv.get(3));
		}

		#[test]
		fn test_from_array_large() {
			let bv = BitVec::from([true; 16]);
			assert_eq!(bv.len(), 16);
			for i in 0..16 {
				assert!(bv.get(i), "expected bit {} to be true", i);
			}
		}

		#[test]
		fn test_from_array_cross_byte() {
			let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
			assert_eq!(bv.len(), 9);
			for i in 0..9 {
				assert_eq!(bv.get(i), i % 2 == 0, "mismatch at bit {}", i);
			}
		}
	}

	mod from_vec {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_from_vec_empty() {
			let bv = BitVec::from(Vec::<bool>::new());
			assert_eq!(bv.len(), 0);
		}

		#[test]
		fn test_from_vec_small() {
			let bv = BitVec::from(vec![true, false, true]);
			assert_eq!(bv.len(), 3);
			assert!(bv.get(0));
			assert!(!bv.get(1));
			assert!(bv.get(2));
		}

		#[test]
		fn test_from_vec_large() {
			let input: Vec<bool> = (0..100).map(|i| i % 7 == 0).collect();
			let bv = BitVec::from(input.clone());
			assert_eq!(bv.len(), 100);
			for i in 0..100 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}
	}

	mod empty {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_empty() {
			let bv = BitVec::empty();
			assert_eq!(bv.len(), 0);
			assert!(bv.none());
			assert!(!bv.any());
			assert_eq!(bv.count_ones(), 0);
		}

		#[test]
		fn test_empty_operations() {
			let mut bv = BitVec::empty();

			bv.push(true);
			assert_eq!(bv.len(), 1);
			assert!(bv.get(0));

			let other = BitVec::from([false, true]);
			bv.extend(&other);
			assert_eq!(bv.len(), 3);
			assert!(bv.get(0));
			assert!(!bv.get(1));
			assert!(bv.get(2));
		}
	}

	mod take {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_take_empty() {
			let bv = BitVec::empty();
			let taken = bv.take(5);
			assert_eq!(taken.len(), 0);
		}

		#[test]
		fn test_take_less_than_available() {
			let bv = BitVec::from([true, false, true, false, true]);
			let taken = bv.take(3);
			assert_eq!(taken.len(), 3);
			assert!(taken.get(0));
			assert!(!taken.get(1));
			assert!(taken.get(2));
		}

		#[test]
		fn test_take_exact_length() {
			let bv = BitVec::from([true, false, true]);
			let taken = bv.take(3);
			assert_eq!(taken.len(), 3);
			assert!(taken.get(0));
			assert!(!taken.get(1));
			assert!(taken.get(2));
		}

		#[test]
		fn test_take_more_than_available() {
			let bv = BitVec::from([true, false]);
			let taken = bv.take(5);
			assert_eq!(taken.len(), 2);
			assert!(taken.get(0));
			assert!(!taken.get(1));
		}

		#[test]
		fn test_take_zero() {
			let bv = BitVec::from([true, false, true]);
			let taken = bv.take(0);
			assert_eq!(taken.len(), 0);
		}

		#[test]
		fn test_take_cross_byte_boundary() {
			let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
			let taken = bv.take(6);
			assert_eq!(taken.len(), 6);
			for i in 0..6 {
				assert_eq!(taken.get(i), i % 2 == 0, "mismatch at bit {}", i);
			}
		}
	}

	mod extend {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_extend_empty_to_empty() {
			let mut bv1 = BitVec::empty();
			let bv2 = BitVec::empty();
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 0);
		}

		#[test]
		fn test_extend_empty_to_nonempty() {
			let mut bv1 = BitVec::from([true, false]);
			let bv2 = BitVec::empty();
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 2);
			assert!(bv1.get(0));
			assert!(!bv1.get(1));
		}

		#[test]
		fn test_extend_nonempty_to_empty() {
			let mut bv1 = BitVec::empty();
			let bv2 = BitVec::from([true, false]);
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 2);
			assert!(bv1.get(0));
			assert!(!bv1.get(1));
		}

		#[test]
		fn test_extend_basic() {
			let mut bv1 = BitVec::from([true, false]);
			let bv2 = BitVec::from([false, true]);
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 4);
			assert!(bv1.get(0));
			assert!(!bv1.get(1));
			assert!(!bv1.get(2));
			assert!(bv1.get(3));
		}

		#[test]
		fn test_extend_cross_byte_boundary() {
			let mut bv1 = BitVec::from([true, false, true, false, true, false]);
			let bv2 = BitVec::from([false, true, false]);
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 9);

			let expected = [true, false, true, false, true, false, false, true, false];
			for i in 0..9 {
				assert_eq!(bv1.get(i), expected[i], "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_extend_large() {
			let mut bv1 = BitVec::from_fn(50, |i| i % 2 == 0);
			let bv2 = BitVec::from_fn(50, |i| i % 3 == 0);
			bv1.extend(&bv2);
			assert_eq!(bv1.len(), 100);

			for i in 0..50 {
				assert_eq!(bv1.get(i), i % 2 == 0, "first half mismatch at bit {}", i);
			}
			for i in 50..100 {
				assert_eq!(bv1.get(i), (i - 50) % 3 == 0, "second half mismatch at bit {}", i);
			}
		}
	}

	mod push {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_push_to_empty() {
			let mut bv = BitVec::empty();
			bv.push(true);
			assert_eq!(bv.len(), 1);
			assert!(bv.get(0));
		}

		#[test]
		fn test_push_alternating() {
			let mut bv = BitVec::empty();
			for i in 0..10 {
				bv.push(i % 2 == 0);
			}
			assert_eq!(bv.len(), 10);
			for i in 0..10 {
				assert_eq!(bv.get(i), i % 2 == 0, "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_push_cross_byte_boundary() {
			let mut bv = BitVec::empty();
			for i in 0..17 {
				bv.push(i % 3 == 0);
			}
			assert_eq!(bv.len(), 17);
			for i in 0..17 {
				assert_eq!(bv.get(i), i % 3 == 0, "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_push_many() {
			let mut bv = BitVec::empty();
			for i in 0..1000 {
				bv.push(i % 7 == 0);
			}
			assert_eq!(bv.len(), 1000);
			for i in 0..1000 {
				assert_eq!(bv.get(i), i % 7 == 0, "mismatch at bit {}", i);
			}
		}
	}

	mod reorder {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_reorder_identity() {
			let mut bv = BitVec::from([true, false, true, false]);
			bv.reorder(&[0, 1, 2, 3]);
			assert_eq!(bv.len(), 4);
			assert!(bv.get(0));
			assert!(!bv.get(1));
			assert!(bv.get(2));
			assert!(!bv.get(3));
		}

		#[test]
		fn test_reorder_reverse() {
			let mut bv = BitVec::from([true, false, true, false]);
			bv.reorder(&[3, 2, 1, 0]);
			assert_eq!(bv.len(), 4);
			assert!(!bv.get(0)); // was index 3
			assert!(bv.get(1)); // was index 2
			assert!(!bv.get(2)); // was index 1
			assert!(bv.get(3)); // was index 0
		}

		#[test]
		fn test_reorder_custom() {
			let mut bv = BitVec::from([true, false, true, false]);
			bv.reorder(&[2, 0, 3, 1]);
			assert_eq!(bv.len(), 4);
			assert!(bv.get(0)); // was index 2
			assert!(bv.get(1)); // was index 0
			assert!(!bv.get(2)); // was index 3
			assert!(!bv.get(3)); // was index 1
		}

		#[test]
		fn test_reorder_cross_byte_boundary() {
			let mut bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
			bv.reorder(&[8, 7, 6, 5, 4, 3, 2, 1, 0]);
			assert_eq!(bv.len(), 9);

			let expected = [true, false, true, false, true, false, true, false, true]; // reversed
			for i in 0..9 {
				assert_eq!(bv.get(i), expected[8 - i], "mismatch at bit {}", i);
			}
		}

		#[test]
		#[should_panic(expected = "assertion `left == right` failed")]
		fn test_reorder_wrong_length() {
			let mut bv = BitVec::from([true, false, true]);
			bv.reorder(&[0, 1]); // Wrong length should panic
		}
	}

	mod count_ones {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_count_ones_empty() {
			let bv = BitVec::empty();
			assert_eq!(bv.count_ones(), 0);
		}

		#[test]
		fn test_count_ones_all_false() {
			let bv = BitVec::repeat(10, false);
			assert_eq!(bv.count_ones(), 0);
		}

		#[test]
		fn test_count_ones_all_true() {
			let bv = BitVec::repeat(10, true);
			assert_eq!(bv.count_ones(), 10);
		}

		#[test]
		fn test_count_ones_mixed() {
			let bv = BitVec::from([true, false, true, false, true]);
			assert_eq!(bv.count_ones(), 3);
		}

		#[test]
		fn test_count_ones_alternating() {
			let bv = BitVec::from_fn(100, |i| i % 2 == 0);
			assert_eq!(bv.count_ones(), 50);
		}

		#[test]
		fn test_count_ones_cross_byte_boundary() {
			let bv = BitVec::from_fn(17, |i| i % 3 == 0);
			let expected = (0..17).filter(|&i| i % 3 == 0).count();
			assert_eq!(bv.count_ones(), expected);
		}
	}

	mod any_none {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_any_none_empty() {
			let bv = BitVec::empty();
			assert!(!bv.any());
			assert!(bv.none());
		}

		#[test]
		fn test_any_none_all_false() {
			let bv = BitVec::repeat(10, false);
			assert!(!bv.any());
			assert!(bv.none());
		}

		#[test]
		fn test_any_none_all_true() {
			let bv = BitVec::repeat(10, true);
			assert!(bv.any());
			assert!(!bv.none());
		}

		#[test]
		fn test_any_none_mixed() {
			let bv = BitVec::from([false, false, true, false]);
			assert!(bv.any());
			assert!(!bv.none());
		}

		#[test]
		fn test_any_none_single_true() {
			let bv = BitVec::from([true]);
			assert!(bv.any());
			assert!(!bv.none());
		}

		#[test]
		fn test_any_none_single_false() {
			let bv = BitVec::from([false]);
			assert!(!bv.any());
			assert!(bv.none());
		}
	}

	mod to_vec {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_to_vec_empty() {
			let bv = BitVec::empty();
			assert_eq!(bv.to_vec(), Vec::<bool>::new());
		}

		#[test]
		fn test_to_vec_small() {
			let bv = BitVec::from([true, false, true]);
			assert_eq!(bv.to_vec(), vec![true, false, true]);
		}

		#[test]
		fn test_to_vec_cross_byte_boundary() {
			let input = [true, false, true, false, true, false, true, false, true];
			let bv = BitVec::from(input);
			assert_eq!(bv.to_vec(), input.to_vec());
		}

		#[test]
		fn test_to_vec_large() {
			let input: Vec<bool> = (0..100).map(|i| i % 3 == 0).collect();
			let bv = BitVec::from(input.clone());
			assert_eq!(bv.to_vec(), input);
		}
	}

	mod display {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_display_empty() {
			let bv = BitVec::empty();
			assert_eq!(format!("{}", bv), "");
		}

		#[test]
		fn test_display_small() {
			let bv = BitVec::from([true, false, true]);
			assert_eq!(format!("{}", bv), "101");
		}

		#[test]
		fn test_display_all_false() {
			let bv = BitVec::repeat(5, false);
			assert_eq!(format!("{}", bv), "00000");
		}

		#[test]
		fn test_display_all_true() {
			let bv = BitVec::repeat(5, true);
			assert_eq!(format!("{}", bv), "11111");
		}

		#[test]
		fn test_display_cross_byte_boundary() {
			let bv = BitVec::from([true, false, true, false, true, false, true, false, true]);
			assert_eq!(format!("{}", bv), "101010101");
		}
	}

	mod and_operation {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_and_empty() {
			let a = BitVec::empty();
			let b = BitVec::empty();
			let result = a.and(&b);
			assert_eq!(result.len(), 0);
		}

		#[test]
		fn test_and_all_true() {
			let a = BitVec::repeat(5, true);
			let b = BitVec::repeat(5, true);
			let result = a.and(&b);
			assert_eq!(result.len(), 5);
			for i in 0..5 {
				assert!(result.get(i), "expected bit {} to be true", i);
			}
		}

		#[test]
		fn test_and_all_false() {
			let a = BitVec::repeat(5, false);
			let b = BitVec::repeat(5, false);
			let result = a.and(&b);
			assert_eq!(result.len(), 5);
			for i in 0..5 {
				assert!(!result.get(i), "expected bit {} to be false", i);
			}
		}

		#[test]
		fn test_and_mixed() {
			let a = BitVec::from([true, true, false, false]);
			let b = BitVec::from([true, false, true, false]);
			let result = a.and(&b);
			assert_eq!(result.len(), 4);
			assert!(result.get(0)); // true & true = true
			assert!(!result.get(1)); // true & false = false
			assert!(!result.get(2)); // false & true = false
			assert!(!result.get(3)); // false & false = false
		}

		#[test]
		fn test_and_cross_byte_boundary() {
			let a = BitVec::from_fn(17, |i| i % 2 == 0);
			let b = BitVec::from_fn(17, |i| i % 3 == 0);
			let result = a.and(&b);
			assert_eq!(result.len(), 17);
			for i in 0..17 {
				let expected = (i % 2 == 0) && (i % 3 == 0);
				assert_eq!(result.get(i), expected, "mismatch at bit {}", i);
			}
		}

		#[test]
		#[should_panic(expected = "assertion `left == right` failed")]
		fn test_and_different_lengths() {
			let a = BitVec::repeat(3, true);
			let b = BitVec::repeat(5, true);
			a.and(&b); // Should panic due to different lengths
		}
	}

	mod not_operation {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_empty() {
			let bv = BitVec::empty();
			let result = bv.not();
			assert_eq!(result.len(), 0);
			assert!(result.none());
		}

		#[test]
		fn test_all_true_becomes_all_false() {
			let bv = BitVec::repeat(10, true);
			let result = bv.not();
			assert_eq!(result.len(), 10);
			assert!(result.none());
			for i in 0..10 {
				assert!(!result.get(i), "expected bit {} to be false", i);
			}
		}

		#[test]
		fn test_all_false_becomes_all_true() {
			let bv = BitVec::repeat(10, false);
			let result = bv.not();
			assert_eq!(result.len(), 10);
			assert!(result.all_ones());
			for i in 0..10 {
				assert!(result.get(i), "expected bit {} to be true", i);
			}
		}

		#[test]
		fn test_single_true() {
			let bv = BitVec::from([true]);
			let result = bv.not();
			assert_eq!(result.len(), 1);
			assert!(!result.get(0));
		}

		#[test]
		fn test_single_false() {
			let bv = BitVec::from([false]);
			let result = bv.not();
			assert_eq!(result.len(), 1);
			assert!(result.get(0));
		}

		#[test]
		fn test_alternating() {
			let bv = BitVec::from_slice(&[true, false, true, false, true, false, true, false]);
			let result = bv.not();
			assert_eq!(result.len(), 8);
			for i in 0..8 {
				assert_eq!(result.get(i), i % 2 != 0, "bit {} mismatch", i);
			}
		}

		#[test]
		fn test_partial_byte() {
			// The final partial byte must be masked, or not() sets bits past len.
			let bv = BitVec::from_slice(&[true, false, true, false, true]);
			let result = bv.not();
			assert_eq!(result.len(), 5);
			assert!(!result.get(0));
			assert!(result.get(1));
			assert!(!result.get(2));
			assert!(result.get(3));
			assert!(!result.get(4));
		}

		#[test]
		fn test_exact_byte_boundary() {
			let bv = BitVec::from_slice(&[true, true, true, true, false, false, false, false]);
			let result = bv.not();
			assert_eq!(result.len(), 8);
			for i in 0..4 {
				assert!(!result.get(i), "bit {} should be false", i);
			}
			for i in 4..8 {
				assert!(result.get(i), "bit {} should be true", i);
			}
		}

		#[test]
		fn test_multi_byte_partial() {
			let bv = BitVec::from_fn(13, |i| i < 8);
			let result = bv.not();
			assert_eq!(result.len(), 13);
			for i in 0..8 {
				assert!(!result.get(i), "bit {} should be false", i);
			}
			for i in 8..13 {
				assert!(result.get(i), "bit {} should be true", i);
			}
		}

		#[test]
		fn test_large_64bit_chunks() {
			// Over 64 bits, so the word-at-a-time path runs rather than only the byte tail.
			let bv = BitVec::from_fn(100, |i| i % 3 == 0);
			let result = bv.not();
			assert_eq!(result.len(), 100);
			for i in 0..100 {
				assert_eq!(result.get(i), i % 3 != 0, "bit {} mismatch", i);
			}
		}

		#[test]
		fn test_double_not_is_identity() {
			let bv = BitVec::from_fn(37, |i| i % 5 < 2);
			let result = bv.not().not();
			assert_eq!(bv.to_vec(), result.to_vec());
		}

		#[test]
		fn test_not_and_or_demorgan() {
			let a = BitVec::from_fn(20, |i| i % 2 == 0);
			let b = BitVec::from_fn(20, |i| i % 3 == 0);

			let lhs = a.and(&b).not();
			let rhs = a.not().or(&b.not());
			assert_eq!(lhs.to_vec(), rhs.to_vec());
		}

		#[test]
		fn test_not_preserves_count() {
			let bv = BitVec::from_fn(50, |i| i < 20);
			let result = bv.not();
			assert_eq!(bv.count_ones() + result.count_ones(), 50);
			assert_eq!(bv.count_zeros() + result.count_zeros(), 50);
		}
	}

	mod edge_cases {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_single_bit_operations() {
			let mut bv = BitVec::from([true]);
			assert_eq!(bv.len(), 1);
			assert!(bv.get(0));
			assert_eq!(bv.count_ones(), 1);
			assert!(bv.any());
			assert!(!bv.none());

			bv.set(0, false);
			assert!(!bv.get(0));
			assert_eq!(bv.count_ones(), 0);
			assert!(!bv.any());
			assert!(bv.none());
		}

		#[test]
		fn test_exactly_one_byte() {
			let input = [true, false, true, false, true, false, true, false];
			let bv = BitVec::from(input);
			assert_eq!(bv.len(), 8);
			for i in 0..8 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_exactly_multiple_bytes() {
			let input: Vec<bool> = (0..16).map(|i| i % 2 == 0).collect();
			let bv = BitVec::from(input.clone());
			assert_eq!(bv.len(), 16);
			for i in 0..16 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_one_bit_past_byte_boundary() {
			let input: Vec<bool> = (0..9).map(|i| i % 2 == 0).collect();
			let bv = BitVec::from(input.clone());
			assert_eq!(bv.len(), 9);
			for i in 0..9 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_seven_bits_in_byte() {
			let input = [true, false, true, false, true, false, true];
			let bv = BitVec::from(input);
			assert_eq!(bv.len(), 7);
			for i in 0..7 {
				assert_eq!(bv.get(i), input[i], "mismatch at bit {}", i);
			}
		}
	}

	mod cow_behavior {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_push_cow() {
			let mut owned = BitVec::with_capacity(16);
			owned.push(true);
			owned.push(false);

			let ptr_before_owned = ptr_of(&owned);
			owned.push(true);
			assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
			assert_eq!(owned.len(), 3);

			let mut shared = owned.clone();

			let ptr_before_shared = ptr_of(&shared);
			shared.push(true);
			assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
			assert_eq!(owned.len(), 3);
			assert_eq!(shared.len(), 4);
		}

		#[test]
		fn test_set_cow() {
			let mut owned = BitVec::repeat(8, false);
			owned.set(1, true);

			let ptr_before_owned = ptr_of(&owned);
			owned.set(2, true);
			assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy

			let mut shared = owned.clone();

			let ptr_before_shared = ptr_of(&shared);
			shared.set(3, true);
			assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
			assert!(!owned.get(3)); // original unchanged
			assert!(shared.get(3)); // new value set
		}

		#[test]
		fn test_extend_cow() {
			let mut owned = BitVec::repeat(4, false);
			let extension = BitVec::repeat(4, true);

			let ptr_before_owned = ptr_of(&owned);
			owned.extend(&extension);
			assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
			assert_eq!(owned.len(), 8);

			let mut shared = owned.clone();

			let ptr_before_shared = ptr_of(&shared);
			shared.extend(&extension);
			assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
			assert_eq!(owned.len(), 8);
			assert_eq!(shared.len(), 12);
		}

		#[test]
		fn test_reorder_cow() {
			let mut owned = BitVec::from_fn(4, |i| i % 2 == 0);

			// reorder allocates a fresh bits array even when the buffer is uniquely owned.
			owned.reorder(&[1, 0, 3, 2]);

			let mut shared = owned.clone();

			let ptr_before_shared = ptr_of(&shared);
			shared.reorder(&[0, 1, 2, 3]); // identity reorder
			assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
		}

		fn ptr_of(v: &BitVec) -> *const u8 {
			v.inner.bits.as_ptr()
		}
	}

	mod stress_tests {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_large_bitvec_operations() {
			let size = 10000;
			let mut bv = BitVec::empty();

			for i in 0..size {
				bv.push(i % 17 == 0);
			}
			assert_eq!(bv.len(), size);

			for i in 0..size {
				assert_eq!(bv.get(i), i % 17 == 0, "mismatch at bit {}", i);
			}

			let expected_ones = (0..size).filter(|&i| i % 17 == 0).count();
			assert_eq!(bv.count_ones(), expected_ones);
		}

		#[test]
		fn test_large_extend_operations() {
			let size = 5000;
			let mut bv1 = BitVec::from_fn(size, |i| i % 13 == 0);
			let bv2 = BitVec::from_fn(size, |i| i % 19 == 0);

			bv1.extend(&bv2);
			assert_eq!(bv1.len(), size * 2);

			for i in 0..size {
				assert_eq!(bv1.get(i), i % 13 == 0, "first half mismatch at bit {}", i);
			}

			for i in size..(size * 2) {
				assert_eq!(bv1.get(i), (i - size) % 19 == 0, "second half mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_many_byte_boundaries() {
			// Sizes straddle every byte and word boundary, where the tail masking goes wrong.
			for size in [7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129] {
				let bv = BitVec::from_fn(size, |i| i % 3 == 0);
				assert_eq!(bv.len(), size);

				for i in 0..size {
					assert_eq!(bv.get(i), i % 3 == 0, "size {} mismatch at bit {}", size, i);
				}

				let expected_ones = (0..size).filter(|&i| i % 3 == 0).count();
				assert_eq!(bv.count_ones(), expected_ones, "count_ones mismatch for size {}", size);
			}
		}

		#[test]
		fn test_multiple_and_operations() {
			let size = 1000;
			let a = BitVec::from_fn(size, |i| i % 2 == 0);
			let b = BitVec::from_fn(size, |i| i % 3 == 0);
			let c = BitVec::from_fn(size, |i| i % 5 == 0);

			let ab = a.and(&b);
			let abc = ab.and(&c);

			assert_eq!(abc.len(), size);
			for i in 0..size {
				let expected = (i % 2 == 0) && (i % 3 == 0) && (i % 5 == 0);
				assert_eq!(abc.get(i), expected, "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_comptokenize_reorder_pattern() {
			let size = 100;
			let mut bv = BitVec::from_fn(size, |i| i % 7 == 0);

			let mut indices: Vec<usize> = (0..size).collect();
			indices.reverse();

			let original_values: Vec<bool> = bv.to_vec();
			bv.reorder(&indices);

			for i in 0..size {
				let original_index = indices[i];
				assert_eq!(
					bv.get(i),
					original_values[original_index],
					"reorder mismatch at position {}",
					i
				);
			}
		}
	}

	mod property_based_tests {
		use crate::util::bitvec::BitVec;

		#[test]
		fn test_roundtrip_conversions() {
			let patterns = [
				vec![],
				vec![true],
				vec![false],
				vec![true, false],
				vec![false, true],
				(0..50).map(|i| i % 2 == 0).collect::<Vec<_>>(),
				(0..50).map(|i| i % 3 == 0).collect::<Vec<_>>(),
				(0..100).map(|i| i % 7 == 0).collect::<Vec<_>>(),
			];

			for pattern in patterns {
				let bv = BitVec::from(pattern.clone());
				let result = bv.to_vec();
				assert_eq!(pattern, result, "roundtrip failed for pattern length {}", pattern.len());

				let bv2 = BitVec::from_slice(&pattern);
				let result2 = bv2.to_vec();
				assert_eq!(
					pattern,
					result2,
					"slice roundtrip failed for pattern length {}",
					pattern.len()
				);

				if pattern.len() <= 32 {
					let bv3 = BitVec::from_slice(&pattern);
					assert_eq!(bv3.len(), pattern.len());
					for (i, &expected) in pattern.iter().enumerate() {
						assert_eq!(
							bv3.get(i),
							expected,
							"array conversion mismatch at bit {}",
							i
						);
					}
				}
			}
		}

		#[test]
		fn test_invariants() {
			let patterns =
				[vec![], vec![true], vec![false], (0..100).map(|i| i % 5 == 0).collect::<Vec<_>>()];

			for pattern in patterns {
				let bv = BitVec::from(pattern.clone());

				assert_eq!(bv.len(), pattern.len());

				let count_ones = bv.count_ones();
				let count_zeros = pattern.iter().filter(|&&b| !b).count();
				assert_eq!(count_ones + count_zeros, pattern.len());

				if count_ones > 0 {
					assert!(bv.any());
					assert!(!bv.none());
				} else {
					assert!(!bv.any());
					assert!(bv.none());
				}

				for (i, &expected) in pattern.iter().enumerate() {
					assert_eq!(bv.get(i), expected, "get() inconsistency at bit {}", i);
				}
			}
		}

		#[test]
		fn test_extend_preserves_original() {
			let original = BitVec::from([true, false, true]);
			let extension = BitVec::from([false, true]);

			let mut extended = original.clone();
			extended.extend(&extension);

			assert_eq!(original.len(), 3);
			assert!(original.get(0));
			assert!(!original.get(1));
			assert!(original.get(2));

			assert_eq!(extended.len(), 5);
			assert!(extended.get(0));
			assert!(!extended.get(1));
			assert!(extended.get(2));
			assert!(!extended.get(3));
			assert!(extended.get(4));
		}

		#[test]
		fn test_and_operation_properties() {
			let a = BitVec::from([true, true, false, false]);
			let b = BitVec::from([true, false, true, false]);

			let result = a.and(&b);

			assert!(result.count_ones() <= a.count_ones());
			assert!(result.count_ones() <= b.count_ones());

			let result2 = b.and(&a);
			assert_eq!(result.to_vec(), result2.to_vec());

			let self_and = a.and(&a);
			assert_eq!(a.to_vec(), self_and.to_vec());
		}
	}
}
