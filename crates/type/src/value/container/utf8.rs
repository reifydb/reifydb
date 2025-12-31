// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{BitVec, CowVec, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Utf8Container {
	data: CowVec<String>,
	bitvec: BitVec,
}

impl Utf8Container {
	pub fn new(data: Vec<String>, bitvec: BitVec) -> Self {
		debug_assert_eq!(data.len(), bitvec.len());
		Self {
			data: CowVec::new(data),
			bitvec,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<String>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(self.data.len(), self.bitvec.len());
		self.data.len()
	}

	pub fn capacity(&self) -> usize {
		debug_assert!(self.data.capacity() >= self.bitvec.capacity());
		self.data.capacity().min(self.bitvec.capacity())
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn push(&mut self, value: String) {
		self.data.push(value);
		self.bitvec.push(true);
	}

	pub fn push_undefined(&mut self) {
		self.data.push(String::new());
		self.bitvec.push(false);
	}

	pub fn get(&self, index: usize) -> Option<&String> {
		if index < self.len() && self.is_defined(index) {
			self.data.get(index)
		} else {
			None
		}
	}

	pub fn bitvec(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn bitvec_mut(&mut self) -> &mut BitVec {
		&mut self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && self.bitvec.get(idx)
	}

	pub fn is_fully_defined(&self) -> bool {
		self.bitvec.count_ones() == self.len()
	}

	pub fn data(&self) -> &CowVec<String> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut CowVec<String> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].clone()
		} else {
			"Undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::Utf8(self.data[index].clone())
		} else {
			Value::Undefined
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.data.extend(other.data.iter().cloned());
		self.bitvec.extend(&other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		self.data.extend(std::iter::repeat(String::new()).take(len));
		self.bitvec.extend(&BitVec::repeat(len, false));
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&String>> + '_ {
		self.data.iter().zip(self.bitvec.iter()).map(|(v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let new_data: Vec<String> = self.data.iter().skip(start).take(end - start).cloned().collect();
		let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
		Self {
			data: CowVec::new(new_data),
			bitvec: BitVec::from_slice(&new_bitvec),
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());
		let mut new_bitvec = BitVec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
				new_bitvec.push(self.bitvec.get(i));
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());
		let mut new_bitvec = BitVec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx].clone());
				new_bitvec.push(self.bitvec.get(idx));
			} else {
				new_data.push(String::new());
				new_bitvec.push(false);
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
			bitvec: self.bitvec.take(num),
		}
	}
}

impl Deref for Utf8Container {
	type Target = [String];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl Default for Utf8Container {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::BitVec;

	#[test]
	fn test_new() {
		let data = vec!["hello".to_string(), "world".to_string(), "test".to_string()];
		let bitvec = BitVec::from_slice(&[true, true, true]);
		let container = Utf8Container::new(data.clone(), bitvec);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"hello".to_string()));
		assert_eq!(container.get(1), Some(&"world".to_string()));
		assert_eq!(container.get(2), Some(&"test".to_string()));
	}

	#[test]
	fn test_from_vec() {
		let data = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
		let container = Utf8Container::from_vec(data);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"foo".to_string()));
		assert_eq!(container.get(1), Some(&"bar".to_string()));
		assert_eq!(container.get(2), Some(&"baz".to_string()));

		// All should be defined
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
		container.push_undefined();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"first".to_string()));
		assert_eq!(container.get(1), Some(&"second".to_string()));
		assert_eq!(container.get(2), None); // undefined

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(!container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = Utf8Container::from_vec(vec!["a".to_string(), "b".to_string()]);
		let container2 = Utf8Container::from_vec(vec!["c".to_string(), "d".to_string()]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 4);
		assert_eq!(container1.get(0), Some(&"a".to_string()));
		assert_eq!(container1.get(1), Some(&"b".to_string()));
		assert_eq!(container1.get(2), Some(&"c".to_string()));
		assert_eq!(container1.get(3), Some(&"d".to_string()));
	}

	#[test]
	fn test_extend_from_undefined() {
		let mut container = Utf8Container::from_vec(vec!["test".to_string()]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"test".to_string()));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), None); // undefined
	}

	#[test]
	fn test_iter() {
		let data = vec!["x".to_string(), "y".to_string(), "z".to_string()];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = Utf8Container::new(data, bitvec);

		let collected: Vec<Option<&String>> = container.iter().collect();
		assert_eq!(collected, vec![Some(&"x".to_string()), None, Some(&"z".to_string())]);
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
		assert_eq!(sliced.get(0), Some(&"two".to_string()));
		assert_eq!(sliced.get(1), Some(&"three".to_string()));
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
		assert_eq!(container.get(0), Some(&"keep".to_string()));
		assert_eq!(container.get(1), Some(&"keep".to_string()));
	}

	#[test]
	fn test_reorder() {
		let mut container =
			Utf8Container::from_vec(vec!["first".to_string(), "second".to_string(), "third".to_string()]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"third".to_string())); // was index 2
		assert_eq!(container.get(1), Some(&"first".to_string())); // was index 0
		assert_eq!(container.get(2), Some(&"second".to_string())); // was index 1
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let mut container = Utf8Container::from_vec(vec!["a".to_string(), "b".to_string()]);
		let indices = [1, 5, 0]; // index 5 is out of bounds

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"b".to_string())); // was index 1
		assert_eq!(container.get(1), None); // out of bounds -> undefined
		assert_eq!(container.get(2), Some(&"a".to_string())); // was index 0
	}

	#[test]
	fn test_empty_strings() {
		let mut container = Utf8Container::with_capacity(2);
		container.push("".to_string()); // empty string
		container.push_undefined();

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&"".to_string()));
		assert_eq!(container.get(1), None);

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_default() {
		let container = Utf8Container::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
