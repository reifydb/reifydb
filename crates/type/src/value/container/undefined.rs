// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{util::bitvec::BitVec, value::Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UndefinedContainer {
	len: usize,
}

impl UndefinedContainer {
	pub fn new(len: usize) -> Self {
		Self {
			len,
		}
	}

	pub fn with_capacity(_capacity: usize) -> Self {
		Self {
			len: 0,
		}
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn capacity(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn clear(&mut self) {
		self.len = 0;
	}

	pub fn is_defined(&self, _idx: usize) -> bool {
		false
	}

	pub fn push_undefined(&mut self) {
		self.len += 1;
	}

	pub fn as_string(&self, _index: usize) -> String {
		"Undefined".to_string()
	}

	pub fn get_value(&self, _index: usize) -> Value {
		Value::Undefined
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.len += other.len;
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		self.len += len;
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			len: (end - start).min(self.len.saturating_sub(start)),
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_len = 0;
		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len {
				new_len += 1;
			}
		}
		self.len = new_len;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		self.len = indices.len();
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			len: num.min(self.len),
		}
	}
}

impl Default for UndefinedContainer {
	fn default() -> Self {
		Self {
			len: 0,
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_new() {
		let container = UndefinedContainer::new(5);
		assert_eq!(container.len(), 5);
		assert!(!container.is_empty());
	}

	#[test]
	fn test_with_capacity() {
		let container = UndefinedContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert_eq!(container.capacity(), 0);
	}

	#[test]
	fn test_push_undefined() {
		let mut container = UndefinedContainer::with_capacity(5);

		container.push_undefined();
		container.push_undefined();
		container.push_undefined();

		assert_eq!(container.len(), 3);
		assert!(!container.is_empty());
	}

	#[test]
	fn test_extend() {
		let mut container1 = UndefinedContainer::new(2);
		let container2 = UndefinedContainer::new(3);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 5);
	}

	#[test]
	fn test_extend_from_undefined() {
		let mut container = UndefinedContainer::new(1);
		container.extend_from_undefined(4);

		assert_eq!(container.len(), 5);
	}

	#[test]
	fn test_slice() {
		let container = UndefinedContainer::new(10);
		let sliced = container.slice(2, 7);

		assert_eq!(sliced.len(), 5);
	}

	#[test]
	fn test_slice_out_of_bounds() {
		let container = UndefinedContainer::new(5);
		let sliced = container.slice(3, 10);

		assert_eq!(sliced.len(), 2); // min(10-3, 5-3) = 2
	}

	#[test]
	fn test_slice_start_beyond_len() {
		let container = UndefinedContainer::new(3);
		let sliced = container.slice(5, 8);

		assert_eq!(sliced.len(), 0); // saturating_sub(3, 5) = 0
	}

	#[test]
	fn test_filter() {
		let mut container = UndefinedContainer::new(6);
		let mask = BitVec::from_slice(&[true, false, true, true, false, true]);

		container.filter(&mask);

		assert_eq!(container.len(), 4); // 4 true data in mask
	}

	#[test]
	fn test_filter_with_mask_longer_than_container() {
		let mut container = UndefinedContainer::new(3);
		let mask = BitVec::from_slice(&[true, false, true, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2); // only first 3 elements matter
	}

	#[test]
	fn test_reorder() {
		let mut container = UndefinedContainer::new(3);
		let indices = [2, 0, 1, 5]; // last index is out of bounds

		container.reorder(&indices);

		assert_eq!(container.len(), 4); // length becomes indices.len()
	}

	#[test]
	fn test_clear() {
		let mut container = UndefinedContainer::new(10);

		container.clear();

		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}

	#[test]
	fn test_capacity_equals_len() {
		let container = UndefinedContainer::new(7);
		assert_eq!(container.capacity(), container.len());
	}

	#[test]
	fn test_default() {
		let container = UndefinedContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert_eq!(container.capacity(), 0);
	}

	#[test]
	fn test_multiple_operations() {
		let mut container = UndefinedContainer::with_capacity(0);

		// Start empty
		assert_eq!(container.len(), 0);

		// Add some undefined data
		container.push_undefined();
		container.push_undefined();
		container.extend_from_undefined(3);
		assert_eq!(container.len(), 5);

		// Filter with mask
		let mask = BitVec::from_slice(&[true, false, true, false, true]);
		container.filter(&mask);
		assert_eq!(container.len(), 3);

		// Reorder
		container.reorder(&[1, 0, 2]);
		assert_eq!(container.len(), 3);

		// Clear
		container.clear();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
