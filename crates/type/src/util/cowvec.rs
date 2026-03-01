// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{borrow::Borrow, mem, ops::Deref, sync::Arc, vec};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::storage::DataVec;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CowVec<T>
where
	T: Clone + PartialEq,
{
	inner: Arc<Vec<T>>,
}

impl<T> CowVec<T>
where
	T: Clone + PartialEq,
{
	pub fn with_capacity(capacity: usize) -> Self {
		// Allocate with extra capacity to ensure alignment for SIMD
		// operations Round up capacity to next multiple of 8 for
		// better cache performance
		let aligned_capacity = (capacity + 7) & !7;
		Self {
			inner: Arc::new(Vec::with_capacity(aligned_capacity)),
		}
	}

	/// Create a new CowVec with aligned capacity for SIMD operations
	pub fn with_aligned_capacity(capacity: usize) -> Self {
		// For SIMD, we want capacity aligned to at least 32 bytes
		// (256-bit SIMD) This ensures we can engine data in chunks
		// without bounds checking
		let simd_alignment = 32 / mem::size_of::<T>().max(1);
		let aligned_capacity = ((capacity + simd_alignment - 1) / simd_alignment) * simd_alignment;
		Self {
			inner: Arc::new(Vec::with_capacity(aligned_capacity)),
		}
	}

	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn capacity(&self) -> usize {
		self.inner.capacity()
	}
}

#[macro_export]
macro_rules! cow_vec {
    () => {
        $crate::util::cowvec::CowVec::new(Vec::new())
    };
    ($($elem:expr),+ $(,)?) => {
        $crate::util::cowvec::CowVec::new(vec![$($elem),+])
    };
}

impl<T> Default for CowVec<T>
where
	T: Clone + PartialEq,
{
	fn default() -> Self {
		Self {
			inner: Arc::new(Vec::new()),
		}
	}
}

impl<T: Clone + PartialEq> PartialEq<[T]> for &CowVec<T> {
	fn eq(&self, other: &[T]) -> bool {
		self.inner.as_slice() == other
	}
}

impl<T: Clone + PartialEq> PartialEq<[T]> for CowVec<T> {
	fn eq(&self, other: &[T]) -> bool {
		self.inner.as_slice() == other
	}
}

impl<T: Clone + PartialEq> PartialEq<CowVec<T>> for [T] {
	fn eq(&self, other: &CowVec<T>) -> bool {
		self == other.inner.as_slice()
	}
}

impl<T: Clone + PartialEq> Clone for CowVec<T> {
	fn clone(&self) -> Self {
		CowVec {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Clone + PartialEq> CowVec<T> {
	pub fn new(vec: Vec<T>) -> Self {
		CowVec {
			inner: Arc::new(vec),
		}
	}

	pub fn from_rc(rc: Arc<Vec<T>>) -> Self {
		CowVec {
			inner: rc,
		}
	}

	/// Try to extract the inner Vec without cloning.
	/// Returns `Ok(Vec<T>)` if this is the sole owner, `Err(self)` otherwise.
	pub fn try_into_vec(self) -> Result<Vec<T>, Self> {
		match Arc::try_unwrap(self.inner) {
			Ok(vec) => Ok(vec),
			Err(arc) => Err(CowVec {
				inner: arc,
			}),
		}
	}

	pub fn as_slice(&self) -> &[T] {
		&self.inner
	}

	pub fn is_owned(&self) -> bool {
		Arc::strong_count(&self.inner) == 1
	}

	pub fn is_shared(&self) -> bool {
		Arc::strong_count(&self.inner) > 1
	}

	pub fn get(&self, idx: usize) -> Option<&T> {
		self.inner.get(idx)
	}

	pub fn make_mut(&mut self) -> &mut Vec<T> {
		Arc::make_mut(&mut self.inner)
	}

	pub fn set(&mut self, idx: usize, value: T) {
		self.make_mut()[idx] = value;
	}

	pub fn push(&mut self, value: T) {
		self.make_mut().push(value);
	}

	/// Clear all elements, retaining the allocated capacity when solely owned.
	pub fn clear(&mut self) {
		self.make_mut().clear();
	}

	pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) {
		self.make_mut().extend(iter);
	}

	pub fn extend_from_slice(&mut self, slice: &[T]) {
		self.make_mut().extend_from_slice(slice);
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let vec = self.make_mut();
		let len = vec.len();
		assert_eq!(len, indices.len());

		let mut visited = vec![false; len];
		for start in 0..len {
			if visited[start] || indices[start] == start {
				continue;
			}
			let mut current = start;
			while !visited[current] {
				visited[current] = true;
				let next = indices[current];
				if next == start {
					break;
				}
				vec.swap(current, next);
				current = next;
			}
		}
	}

	/// Get aligned chunks for SIMD processing
	/// Returns slices that are guaranteed to be aligned and sized for SIMD
	/// operations
	pub fn aligned_chunks(&self, chunk_size: usize) -> impl Iterator<Item = &[T]> {
		self.inner.chunks(chunk_size)
	}

	/// Get mutable aligned chunks for SIMD processing
	pub fn aligned_chunks_mut(&mut self, chunk_size: usize) -> impl Iterator<Item = &mut [T]> {
		self.make_mut().chunks_mut(chunk_size)
	}

	/// Returns true if the data is suitably aligned for SIMD operations
	pub fn is_simd_aligned(&self) -> bool {
		let alignment = 32; // 256-bit SIMD alignment
		let ptr = self.inner.as_ptr() as usize;
		ptr % alignment == 0
	}

	pub fn take(&self, n: usize) -> Self {
		let len = n.min(self.len());
		CowVec::new(self.inner[..len].to_vec())
	}
}

impl<T: Clone + PartialEq> IntoIterator for CowVec<T> {
	type Item = T;
	type IntoIter = vec::IntoIter<T>;

	fn into_iter(self) -> Self::IntoIter {
		match Arc::try_unwrap(self.inner) {
			Ok(vec) => vec.into_iter(),
			Err(arc) => (*arc).clone().into_iter(),
		}
	}
}

impl<T: Clone + PartialEq> Deref for CowVec<T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}

impl<T: Clone + PartialEq> Borrow<[T]> for CowVec<T> {
	fn borrow(&self) -> &[T] {
		self.as_slice()
	}
}

impl<T> Serialize for CowVec<T>
where
	T: Clone + PartialEq + Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.inner.serialize(serializer)
	}
}

impl<'de, T> Deserialize<'de> for CowVec<T>
where
	T: Clone + PartialEq + Deserialize<'de>,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let vec = Vec::<T>::deserialize(deserializer)?;
		Ok(CowVec {
			inner: Arc::new(vec),
		})
	}
}

impl<T: Clone + PartialEq> DataVec<T> for CowVec<T> {
	fn spawn(&self, capacity: usize) -> Self {
		CowVec::with_capacity(capacity)
	}

	fn push(&mut self, value: T) {
		CowVec::push(self, value)
	}

	fn clear(&mut self) {
		CowVec::clear(self)
	}

	fn len(&self) -> usize {
		CowVec::len(self)
	}

	fn as_slice(&self) -> &[T] {
		CowVec::as_slice(self)
	}

	fn get(&self, idx: usize) -> Option<&T> {
		CowVec::get(self, idx)
	}

	fn extend_from_slice(&mut self, other: &[T]) {
		CowVec::extend_from_slice(self, other)
	}

	fn extend_iter(&mut self, iter: impl Iterator<Item = T>) {
		CowVec::extend(self, iter)
	}

	fn capacity(&self) -> usize {
		CowVec::capacity(self)
	}

	fn take(&self, n: usize) -> Self {
		CowVec::take(self, n)
	}
}

#[cfg(test)]
pub mod tests {
	use super::CowVec;

	#[test]
	fn test_new() {
		let cow = CowVec::new(vec![1, 2, 3]);
		assert_eq!(cow.get(0), Some(&1));
		assert_eq!(cow.get(1), Some(&2));
		assert_eq!(cow.get(2), Some(&3));
	}

	#[test]
	fn test_is_owned() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		assert!(owned.is_owned());

		let shared = owned.clone();
		assert!(!owned.is_owned());
		assert!(!shared.is_owned());

		drop(shared);

		assert!(owned.is_owned());
	}

	#[test]
	fn test_is_shared() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		assert!(!owned.is_shared());

		let shared = owned.clone();
		assert!(owned.is_shared());
		assert!(shared.is_shared());

		drop(shared);

		assert!(!owned.is_shared());
	}

	#[test]
	fn test_extend() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		let ptr_before_owned = ptr_of(&owned);
		owned.extend([9, 9, 24]);
		assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
		assert_eq!(owned.len(), 5);

		let mut shared = owned.clone();

		let ptr_before_shared = ptr_of(&shared);
		shared.extend([9, 9, 24]);
		assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
		assert_eq!(owned.len(), 5);
	}

	#[test]
	fn test_push() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		let ptr_before_owned = ptr_of(&owned);
		owned.push(99);
		assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
		assert_eq!(owned.len(), 3);

		let mut shared = owned.clone();

		let ptr_before_shared = ptr_of(&shared);
		shared.push(99);
		assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
		assert_eq!(owned.len(), 3);
	}

	#[test]
	fn test_set() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		let ptr_before_owned = ptr_of(&owned);
		owned.set(1, 99);
		assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
		assert_eq!(*owned, [1, 99]);

		let mut shared = owned.clone();

		let ptr_before_shared = ptr_of(&shared);
		shared.set(1, 99);
		assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
		assert_eq!(*owned, [1, 99]);
	}

	#[test]
	fn test_reorder() {
		let mut owned = CowVec::new(Vec::with_capacity(16));
		owned.extend([1, 2]);

		let ptr_before_owned = ptr_of(&owned);
		owned.reorder(&[1usize, 0]);
		assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
		assert_eq!(*owned, [2, 1]);

		let mut shared = owned.clone();

		let ptr_before_shared = ptr_of(&shared);
		shared.reorder(&[1usize, 0]);
		assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
		assert_eq!(*shared, [1, 2]);
	}

	#[test]
	fn test_reorder_identity() {
		let mut cow = CowVec::new(vec![10, 20, 30]);
		cow.reorder(&[0, 1, 2]); // no-op
		assert_eq!(cow.as_slice(), &[10, 20, 30]);
	}

	#[test]
	fn test_reorder_basic() {
		let mut cow = CowVec::new(vec![10, 20, 30]);
		cow.reorder(&[2, 0, 1]);
		assert_eq!(cow.as_slice(), &[30, 10, 20]);
	}

	fn ptr_of(v: &CowVec<i32>) -> *const i32 {
		v.as_slice().as_ptr()
	}
}
