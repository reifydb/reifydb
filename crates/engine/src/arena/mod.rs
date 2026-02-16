// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
};

use reifydb_type::storage::{DataBitVec, DataVec, Storage};

pub mod convert;

pub struct BumpVec<'bump, T: Clone + PartialEq> {
	inner: bumpalo::collections::Vec<'bump, T>,
}

impl<'bump, T: Clone + PartialEq + Debug> Debug for BumpVec<'bump, T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("BumpVec").field(&self.inner.as_slice()).finish()
	}
}

impl<'bump, T: Clone + PartialEq> BumpVec<'bump, T> {
	pub fn with_capacity_in(capacity: usize, bump: &'bump bumpalo::Bump) -> Self {
		Self {
			inner: bumpalo::collections::Vec::with_capacity_in(capacity, bump),
		}
	}

	fn bump(&self) -> &'bump bumpalo::Bump {
		self.inner.bump()
	}
}

impl<'bump, T: Clone + PartialEq> Clone for BumpVec<'bump, T> {
	fn clone(&self) -> Self {
		let mut new = bumpalo::collections::Vec::with_capacity_in(self.inner.len(), self.bump());
		new.extend(self.inner.iter().cloned());
		Self {
			inner: new,
		}
	}
}

impl<'bump, T: Clone + PartialEq> PartialEq for BumpVec<'bump, T> {
	fn eq(&self, other: &Self) -> bool {
		self.inner.as_slice() == other.inner.as_slice()
	}
}

impl<'bump, T: Clone + PartialEq> Deref for BumpVec<'bump, T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.inner.as_slice()
	}
}

impl<'bump, T: Clone + PartialEq> DataVec<T> for BumpVec<'bump, T> {
	fn spawn(&self, capacity: usize) -> Self {
		Self {
			inner: bumpalo::collections::Vec::with_capacity_in(capacity, self.bump()),
		}
	}

	fn push(&mut self, value: T) {
		self.inner.push(value);
	}

	fn clear(&mut self) {
		self.inner.clear();
	}

	fn len(&self) -> usize {
		self.inner.len()
	}

	fn as_slice(&self) -> &[T] {
		self.inner.as_slice()
	}

	fn get(&self, idx: usize) -> Option<&T> {
		self.inner.get(idx)
	}

	fn extend_from_slice(&mut self, other: &[T]) {
		self.inner.extend(other.iter().cloned());
	}

	fn extend_iter(&mut self, iter: impl Iterator<Item = T>) {
		self.inner.extend(iter);
	}

	fn capacity(&self) -> usize {
		self.inner.capacity()
	}
}

pub struct BumpBitVec<'bump> {
	bits: bumpalo::collections::Vec<'bump, u8>,
	len: usize,
}

impl<'bump> Debug for BumpBitVec<'bump> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BumpBitVec").field("len", &self.len).finish()
	}
}

impl<'bump> BumpBitVec<'bump> {
	pub fn with_capacity_in(capacity: usize, bump: &'bump bumpalo::Bump) -> Self {
		let byte_capacity = (capacity + 7) / 8;
		Self {
			bits: bumpalo::collections::Vec::with_capacity_in(byte_capacity, bump),
			len: 0,
		}
	}

	fn bump(&self) -> &'bump bumpalo::Bump {
		self.bits.bump()
	}
}

impl<'bump> Clone for BumpBitVec<'bump> {
	fn clone(&self) -> Self {
		let mut new_bits = bumpalo::collections::Vec::with_capacity_in(self.bits.len(), self.bump());
		new_bits.extend(self.bits.iter().copied());
		Self {
			bits: new_bits,
			len: self.len,
		}
	}
}

impl<'bump> PartialEq for BumpBitVec<'bump> {
	fn eq(&self, other: &Self) -> bool {
		if self.len != other.len {
			return false;
		}
		// Compare full bytes
		let full_bytes = self.len / 8;
		if self.bits[..full_bytes] != other.bits[..full_bytes] {
			return false;
		}
		// Compare remaining bits in last partial byte
		let remainder = self.len % 8;
		if remainder > 0 {
			let mask = (1u8 << remainder) - 1;
			let a = self.bits[full_bytes] & mask;
			let b = other.bits[full_bytes] & mask;
			if a != b {
				return false;
			}
		}
		true
	}
}

impl<'bump> DataBitVec for BumpBitVec<'bump> {
	fn spawn(&self, capacity: usize) -> Self {
		BumpBitVec::with_capacity_in(capacity, self.bump())
	}

	fn push(&mut self, bit: bool) {
		let byte_index = self.len / 8;
		let bit_index = self.len % 8;

		if byte_index >= self.bits.len() {
			self.bits.push(0);
		}

		if bit {
			self.bits[byte_index] |= 1 << bit_index;
		}

		self.len += 1;
	}

	fn get(&self, idx: usize) -> bool {
		assert!(idx < self.len);
		let byte = self.bits[idx / 8];
		let bit = idx % 8;
		(byte >> bit) & 1 != 0
	}

	fn set(&mut self, idx: usize, value: bool) {
		assert!(idx < self.len);
		let byte = &mut self.bits[idx / 8];
		let bit = idx % 8;
		if value {
			*byte |= 1 << bit;
		} else {
			*byte &= !(1 << bit);
		}
	}

	fn len(&self) -> usize {
		self.len
	}

	fn clear(&mut self) {
		self.bits.clear();
		self.len = 0;
	}

	fn extend_from(&mut self, other: &Self) {
		for i in 0..other.len {
			self.push(DataBitVec::get(other, i));
		}
	}

	fn count_ones(&self) -> usize {
		let mut count: usize = self.bits.iter().map(|&byte| byte.count_ones() as usize).sum();

		// Adjust for partial last byte
		let full_bytes = self.len / 8;
		let remainder_bits = self.len % 8;

		if remainder_bits > 0 && full_bytes < self.bits.len() {
			let last_byte = self.bits[full_bytes];
			let mask = (1u8 << remainder_bits) - 1;
			count -= (last_byte & !mask).count_ones() as usize;
		}

		count
	}

	fn iter(&self) -> impl Iterator<Item = bool> + '_ {
		(0..self.len).map(|i| {
			let byte = self.bits[i / 8];
			let bit = i % 8;
			(byte >> bit) & 1 != 0
		})
	}

	fn capacity(&self) -> usize {
		self.bits.capacity() * 8
	}
}

#[derive(Clone)]
pub struct Bump<'bump>(&'bump bumpalo::Bump);

impl<'bump> Bump<'bump> {
	pub fn new(bump: &'bump bumpalo::Bump) -> Self {
		Self(bump)
	}

	pub fn inner(&self) -> &'bump bumpalo::Bump {
		self.0
	}
}

impl<'bump> Storage for Bump<'bump> {
	type Vec<T: Clone + PartialEq + 'static> = BumpVec<'bump, T>;
	type BitVec = BumpBitVec<'bump>;
}

pub struct QueryArena {
	bump: bumpalo::Bump,
}

impl QueryArena {
	pub fn new() -> Self {
		Self {
			bump: bumpalo::Bump::with_capacity(64 * 1024),
		}
	}

	pub fn reset(&mut self) {
		self.bump.reset();
	}

	pub fn bump(&self) -> &bumpalo::Bump {
		&self.bump
	}
}

impl Default for QueryArena {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::storage::{DataBitVec, DataVec};

	use super::*;

	mod bump_vec {
		use super::*;

		#[test]
		fn test_push_and_get() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v.push(10);
			v.push(20);
			v.push(30);

			assert_eq!(DataVec::len(&v), 3);
			assert_eq!(DataVec::get(&v, 0), Some(&10));
			assert_eq!(DataVec::get(&v, 1), Some(&20));
			assert_eq!(DataVec::get(&v, 2), Some(&30));
			assert_eq!(DataVec::get(&v, 3), None);
		}

		#[test]
		fn test_extend_from_slice() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(8, &bump);
			v.push(1);
			DataVec::extend_from_slice(&mut v, &[2, 3, 4]);
			assert_eq!(v.as_slice(), &[1, 2, 3, 4]);
		}

		#[test]
		fn test_spawn() {
			let bump = bumpalo::Bump::new();
			let v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			let v2 = DataVec::<i32>::spawn(&v, 8);
			assert_eq!(DataVec::len(&v2), 0);
			assert!(DataVec::capacity(&v2) >= 8);
		}

		#[test]
		fn test_clone() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v.push(1);
			v.push(2);

			let v2 = v.clone();
			assert_eq!(v.as_slice(), v2.as_slice());
		}

		#[test]
		fn test_clear() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v.push(1);
			v.push(2);
			DataVec::clear(&mut v);
			assert_eq!(DataVec::len(&v), 0);
		}

		#[test]
		fn test_deref() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v.push(10);
			v.push(20);
			let slice: &[i32] = &*v;
			assert_eq!(slice, &[10, 20]);
		}

		#[test]
		fn test_eq() {
			let bump = bumpalo::Bump::new();
			let mut v1: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v1.push(1);
			v1.push(2);

			let mut v2: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v2.push(1);
			v2.push(2);

			assert_eq!(v1, v2);

			v2.push(3);
			assert_ne!(v1, v2);
		}

		#[test]
		fn test_take() {
			let bump = bumpalo::Bump::new();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, &bump);
			v.push(10);
			v.push(20);
			v.push(30);

			let taken = DataVec::take(&v, 2);
			assert_eq!(taken.as_slice(), &[10, 20]);

			let taken_all = DataVec::take(&v, 5);
			assert_eq!(taken_all.as_slice(), &[10, 20, 30]);
		}
	}

	mod bump_bitvec {
		use super::*;

		#[test]
		fn test_push_and_get() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(true);
			bv.push(false);
			bv.push(true);

			assert_eq!(DataBitVec::len(&bv), 3);
			assert!(DataBitVec::get(&bv, 0));
			assert!(!DataBitVec::get(&bv, 1));
			assert!(DataBitVec::get(&bv, 2));
		}

		#[test]
		fn test_set() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(false);
			bv.push(false);
			bv.push(false);

			DataBitVec::set(&mut bv, 1, true);
			assert!(!DataBitVec::get(&bv, 0));
			assert!(DataBitVec::get(&bv, 1));
			assert!(!DataBitVec::get(&bv, 2));

			DataBitVec::set(&mut bv, 1, false);
			assert!(!DataBitVec::get(&bv, 1));
		}

		#[test]
		fn test_cross_byte_boundary() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(16, &bump);
			for i in 0..17 {
				bv.push(i % 3 == 0);
			}

			assert_eq!(DataBitVec::len(&bv), 17);
			for i in 0..17 {
				assert_eq!(DataBitVec::get(&bv, i), i % 3 == 0, "mismatch at bit {}", i);
			}
		}

		#[test]
		fn test_count_ones() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(16, &bump);
			bv.push(true);
			bv.push(false);
			bv.push(true);
			bv.push(false);
			bv.push(true);

			assert_eq!(DataBitVec::count_ones(&bv), 3);
			assert_eq!(DataBitVec::count_zeros(&bv), 2);
		}

		#[test]
		fn test_count_ones_cross_byte() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(16, &bump);
			for i in 0..17 {
				bv.push(i % 3 == 0);
			}
			let expected = (0..17).filter(|&i| i % 3 == 0).count();
			assert_eq!(DataBitVec::count_ones(&bv), expected);
		}

		#[test]
		fn test_extend_from() {
			let bump = bumpalo::Bump::new();
			let mut bv1 = BumpBitVec::with_capacity_in(8, &bump);
			bv1.push(true);
			bv1.push(false);

			let mut bv2 = BumpBitVec::with_capacity_in(8, &bump);
			bv2.push(false);
			bv2.push(true);

			DataBitVec::extend_from(&mut bv1, &bv2);
			assert_eq!(DataBitVec::len(&bv1), 4);
			assert!(DataBitVec::get(&bv1, 0));
			assert!(!DataBitVec::get(&bv1, 1));
			assert!(!DataBitVec::get(&bv1, 2));
			assert!(DataBitVec::get(&bv1, 3));
		}

		#[test]
		fn test_clone() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(true);
			bv.push(false);
			bv.push(true);

			let bv2 = bv.clone();
			assert_eq!(DataBitVec::len(&bv2), 3);
			assert!(DataBitVec::get(&bv2, 0));
			assert!(!DataBitVec::get(&bv2, 1));
			assert!(DataBitVec::get(&bv2, 2));
		}

		#[test]
		fn test_clear() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(true);
			bv.push(false);
			DataBitVec::clear(&mut bv);
			assert_eq!(DataBitVec::len(&bv), 0);
		}

		#[test]
		fn test_spawn() {
			let bump = bumpalo::Bump::new();
			let bv = BumpBitVec::with_capacity_in(8, &bump);
			let bv2 = DataBitVec::spawn(&bv, 16);
			assert_eq!(DataBitVec::len(&bv2), 0);
			assert!(DataBitVec::capacity(&bv2) >= 16);
		}

		#[test]
		fn test_iter() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(true);
			bv.push(false);
			bv.push(true);
			bv.push(false);

			let collected: Vec<bool> = DataBitVec::iter(&bv).collect();
			assert_eq!(collected, vec![true, false, true, false]);
		}

		#[test]
		fn test_eq() {
			let bump = bumpalo::Bump::new();
			let mut bv1 = BumpBitVec::with_capacity_in(8, &bump);
			bv1.push(true);
			bv1.push(false);

			let mut bv2 = BumpBitVec::with_capacity_in(8, &bump);
			bv2.push(true);
			bv2.push(false);

			assert_eq!(bv1, bv2);

			bv2.push(true);
			assert_ne!(bv1, bv2);
		}

		#[test]
		fn test_take() {
			let bump = bumpalo::Bump::new();
			let mut bv = BumpBitVec::with_capacity_in(8, &bump);
			bv.push(true);
			bv.push(false);
			bv.push(true);

			let taken = DataBitVec::take(&bv, 2);
			assert_eq!(DataBitVec::len(&taken), 2);
			assert!(DataBitVec::get(&taken, 0));
			assert!(!DataBitVec::get(&taken, 1));

			let taken_all = DataBitVec::take(&bv, 5);
			assert_eq!(DataBitVec::len(&taken_all), 3);
		}
	}

	mod bump_storage {
		use reifydb_type::value::container::number::NumberContainer;

		use super::*;

		#[test]
		fn test_number_container_with_bump() {
			let bump_alloc = bumpalo::Bump::new();
			let data = BumpVec::with_capacity_in(4, &bump_alloc);
			let mut container: NumberContainer<i32, Bump<'_>> = NumberContainer::from_parts(data);

			container.push(42);
			container.push(99);

			assert_eq!(container.len(), 2);
			assert_eq!(container.get(0), Some(&42));
			assert_eq!(container.get(1), Some(&99));
		}

		#[test]
		fn test_bool_container_with_bump() {
			use reifydb_type::value::container::bool::BoolContainer;

			let bump_alloc = bumpalo::Bump::new();
			let data = BumpBitVec::with_capacity_in(4, &bump_alloc);
			let mut container: BoolContainer<Bump<'_>> = BoolContainer::from_parts(data);

			container.push(true);
			container.push(false);
			container.push_default();

			assert_eq!(container.len(), 3);
			assert_eq!(container.get(0), Some(true));
			assert_eq!(container.get(1), Some(false));
			// push_default on a bare container pushes false;
			// nullability is tracked by the Option wrapper at the ColumnData level.
			assert_eq!(container.get(2), Some(false));
		}

		#[test]
		fn test_query_arena_reset() {
			let mut arena = QueryArena::new();
			let bump = arena.bump();
			let mut v: BumpVec<i32> = BumpVec::with_capacity_in(4, bump);
			v.push(1);
			v.push(2);
			assert_eq!(DataVec::len(&v), 2);

			// After reset, the arena memory is reclaimed
			drop(v);
			arena.reset();

			let bump = arena.bump();
			let v2: BumpVec<i32> = BumpVec::with_capacity_in(4, bump);
			assert_eq!(DataVec::len(&v2), 0);
		}
	}
}
