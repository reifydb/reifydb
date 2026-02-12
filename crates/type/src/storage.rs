// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use crate::util::{bitvec::BitVec, cowvec::CowVec};

/// Trait for vector-like storage of column values.
pub trait DataVec<T: Clone>: Deref<Target = [T]> + Clone {
	/// Create a new empty vec with given capacity, using the same allocator.
	fn spawn(&self, capacity: usize) -> Self;
	fn push(&mut self, value: T);
	fn clear(&mut self);
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool {
		self.len() == 0
	}
	fn as_slice(&self) -> &[T];
	fn get(&self, idx: usize) -> Option<&T>;
	fn extend_from_slice(&mut self, other: &[T]);
	fn extend_iter(&mut self, iter: impl Iterator<Item = T>);
	fn capacity(&self) -> usize;
	fn take(&self, n: usize) -> Self {
		let len = n.min(self.len());
		let mut new = self.spawn(len);
		new.extend_from_slice(&self.as_slice()[..len]);
		new
	}
}

/// Trait for bitvec-like storage (null masks and boolean data).
pub trait DataBitVec: Clone {
	fn spawn(&self, capacity: usize) -> Self;
	fn push(&mut self, bit: bool);
	fn get(&self, idx: usize) -> bool;
	fn set(&mut self, idx: usize, value: bool);
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool {
		self.len() == 0
	}
	fn clear(&mut self);
	fn extend_from(&mut self, other: &Self);
	fn count_ones(&self) -> usize;
	fn count_zeros(&self) -> usize {
		self.len() - self.count_ones()
	}
	fn iter(&self) -> impl Iterator<Item = bool> + '_;
	fn capacity(&self) -> usize;
	fn take(&self, n: usize) -> Self {
		let len = n.min(self.len());
		let mut new = self.spawn(len);
		for i in 0..len {
			new.push(self.get(i));
		}
		new
	}
}

/// Trait abstracting over storage backends.
pub trait Storage: Clone {
	type Vec<T: Clone + PartialEq + 'static>: DataVec<T> + PartialEq;
	type BitVec: DataBitVec + PartialEq;
}

/// Default storage backend using Arc-backed copy-on-write.
#[derive(Clone, Debug)]
pub struct Cow;

impl Storage for Cow {
	type Vec<T: Clone + PartialEq + 'static> = CowVec<T>;
	type BitVec = BitVec;
}
