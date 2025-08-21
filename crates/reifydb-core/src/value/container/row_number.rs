// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{BitVec, CowVec, RowNumber, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowNumberContainer {
	data: CowVec<RowNumber>,
	bitvec: BitVec,
}

impl Deref for RowNumberContainer {
	type Target = [RowNumber];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl RowNumberContainer {
	pub fn new(data: Vec<RowNumber>, bitvec: BitVec) -> Self {
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

	pub fn from_vec(data: Vec<RowNumber>) -> Self {
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

	pub fn push(&mut self, value: RowNumber) {
		self.data.push(value);
		self.bitvec.push(true);
	}

	pub fn push_undefined(&mut self) {
		self.data.push(RowNumber::default());
		self.bitvec.push(false);
	}

	pub fn get(&self, index: usize) -> Option<&RowNumber> {
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

	pub fn data(&self) -> &CowVec<RowNumber> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut CowVec<RowNumber> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			"Undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::RowNumber(self.data[index])
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
		self.data
			.extend(std::iter::repeat(RowNumber::default())
				.take(len));
		self.bitvec.extend(&BitVec::repeat(len, false));
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<RowNumber>> + '_ {
		self.data.iter().zip(self.bitvec.iter()).map(|(&v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let new_data: Vec<RowNumber> = self
			.data
			.iter()
			.skip(start)
			.take(end - start)
			.cloned()
			.collect();
		let new_bitvec: Vec<bool> = self
			.bitvec
			.iter()
			.skip(start)
			.take(end - start)
			.collect();
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
				new_data.push(RowNumber::default());
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

impl Default for RowNumberContainer {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_new() {
		let row_number1 = RowNumber::new(1);
		let row_number2 = RowNumber::new(2);
		let row_numbers = vec![row_number1, row_number2];
		let bitvec = BitVec::from_slice(&[true, true]);
		let container =
			RowNumberContainer::new(row_numbers.clone(), bitvec);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&row_numbers[0]));
		assert_eq!(container.get(1), Some(&row_numbers[1]));
	}

	#[test]
	fn test_from_vec() {
		let row_numbers = vec![
			RowNumber::new(10),
			RowNumber::new(20),
			RowNumber::new(30),
		];
		let container =
			RowNumberContainer::from_vec(row_numbers.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&row_numbers[0]));
		assert_eq!(container.get(1), Some(&row_numbers[1]));
		assert_eq!(container.get(2), Some(&row_numbers[2]));

		// All should be defined
		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container = RowNumberContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_with_undefined() {
		let mut container = RowNumberContainer::with_capacity(3);
		let row_number1 = RowNumber::new(100);
		let row_number2 = RowNumber::new(200);

		container.push(row_number1);
		container.push_undefined();
		container.push(row_number2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&row_number1));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), Some(&row_number2));

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let row_number1 = RowNumber::new(1);
		let row_number2 = RowNumber::new(2);
		let row_number3 = RowNumber::new(3);

		let mut container1 = RowNumberContainer::from_vec(vec![
			row_number1,
			row_number2,
		]);
		let container2 =
			RowNumberContainer::from_vec(vec![row_number3]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 3);
		assert_eq!(container1.get(0), Some(&row_number1));
		assert_eq!(container1.get(1), Some(&row_number2));
		assert_eq!(container1.get(2), Some(&row_number3));
	}

	#[test]
	fn test_extend_from_undefined() {
		let row_number = RowNumber::new(42);
		let mut container =
			RowNumberContainer::from_vec(vec![row_number]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&row_number));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), None); // undefined
	}

	#[test]
	fn test_iter() {
		let row_number1 = RowNumber::new(10);
		let row_number2 = RowNumber::new(20);
		let row_number3 = RowNumber::new(30);
		let row_numbers = vec![row_number1, row_number2, row_number3];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container =
			RowNumberContainer::new(row_numbers.clone(), bitvec);

		let collected: Vec<Option<RowNumber>> =
			container.iter().collect();
		assert_eq!(
			collected,
			vec![Some(row_numbers[0]), None, Some(row_numbers[2])]
		);
	}

	#[test]
	fn test_slice() {
		let container = RowNumberContainer::from_vec(vec![
			RowNumber::new(1),
			RowNumber::new(2),
			RowNumber::new(3),
			RowNumber::new(4),
		]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&RowNumber::new(2)));
		assert_eq!(sliced.get(1), Some(&RowNumber::new(3)));
	}

	#[test]
	fn test_filter() {
		let mut container = RowNumberContainer::from_vec(vec![
			RowNumber::new(1),
			RowNumber::new(2),
			RowNumber::new(3),
			RowNumber::new(4),
		]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&RowNumber::new(1)));
		assert_eq!(container.get(1), Some(&RowNumber::new(3)));
	}

	#[test]
	fn test_reorder() {
		let mut container = RowNumberContainer::from_vec(vec![
			RowNumber::new(10),
			RowNumber::new(20),
			RowNumber::new(30),
		]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&RowNumber::new(30))); // was index 2
		assert_eq!(container.get(1), Some(&RowNumber::new(10))); // was index 0
		assert_eq!(container.get(2), Some(&RowNumber::new(20))); // was index 1
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let mut container = RowNumberContainer::from_vec(vec![
			RowNumber::new(1),
			RowNumber::new(2),
		]);
		let indices = [1, 5, 0]; // index 5 is out of bounds

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&RowNumber::new(2))); // was index 1
		assert_eq!(container.get(1), None); // out of bounds -> undefined
		assert_eq!(container.get(2), Some(&RowNumber::new(1))); // was index 0
	}

	#[test]
	fn test_default() {
		let container = RowNumberContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}

	#[test]
	fn test_data_access() {
		let mut container = RowNumberContainer::from_vec(vec![
			RowNumber::new(1),
			RowNumber::new(2),
		]);

		// Test immutable access
		assert_eq!(container.data().len(), 2);

		// Test mutable access
		container.data_mut().push(RowNumber::new(3));
		container.bitvec_mut().push(true);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(2), Some(&RowNumber::new(3)));
	}
}
