// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	util::bitvec::BitVec,
	value::{Value, value_type::ValueType},
};

pub struct BoolContainer {
	data: BitVec,
}

impl Clone for BoolContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl Debug for BoolContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BoolContainer").field("data", &self.data).finish()
	}
}

impl PartialEq for BoolContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for BoolContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a BitVec,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for BoolContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(BoolContainer {
			data: h.data,
		})
	}
}

impl Deref for BoolContainer {
	type Target = BitVec;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl BoolContainer {
	pub fn new(data: Vec<bool>) -> Self {
		Self {
			data: BitVec::from_slice(&data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: BitVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<bool>) -> Self {
		Self {
			data: BitVec::from_slice(&data),
		}
	}
}

impl BoolContainer {
	pub fn from_parts(data: BitVec) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn heap_size(&self) -> usize {
		self.capacity().div_ceil(8)
	}

	pub fn is_empty(&self) -> bool {
		self.data.len() == 0
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: bool) {
		self.data.push(value);
	}

	pub fn push_default(&mut self) {
		self.data.push(false);
	}

	pub fn get(&self, index: usize) -> Option<bool> {
		if index < self.len() {
			Some(self.data.get(index))
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn is_fully_defined(&self) -> bool {
		true
	}

	pub fn data(&self) -> &BitVec {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut BitVec {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data.get(index).to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			Value::Boolean(self.data.get(index))
		} else {
			Value::none_of(ValueType::Boolean)
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend(&other.data);
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
		self.data.iter().map(Some)
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = BitVec::with_capacity(count);
		for i in start..(start + count) {
			new_data.push(self.data.get(i));
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = BitVec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data.get(i));
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = BitVec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data.get(idx));
			} else {
				new_data.push(false);
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
		}
	}
}

impl IntoIterator for BoolContainer {
	type Item = Option<bool>;
	type IntoIter = std::iter::Map<std::vec::IntoIter<bool>, fn(bool) -> Option<bool>>;

	fn into_iter(self) -> Self::IntoIter {
		let data: Vec<bool> = self.data.iter().collect();
		data.into_iter().map(Some as fn(bool) -> Option<bool>)
	}
}

impl Default for BoolContainer {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_new() {
		let data = vec![true, false, true];
		let container = BoolContainer::new(data.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(true));
		assert_eq!(container.get(1), Some(false));
		assert_eq!(container.get(2), Some(true));
	}

	#[test]
	fn test_from_vec() {
		let data = vec![true, false, true];
		let container = BoolContainer::from_vec(data);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(true));
		assert_eq!(container.get(1), Some(false));
		assert_eq!(container.get(2), Some(true));

		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container = BoolContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push() {
		let mut container = BoolContainer::with_capacity(3);

		container.push(true);
		container.push(false);
		container.push_default();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(true));
		assert_eq!(container.get(1), Some(false));
		assert_eq!(container.get(2), Some(false)); // default pushes false

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = BoolContainer::from_vec(vec![true, false]);
		let container2 = BoolContainer::from_vec(vec![false, true]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 4);
		assert_eq!(container1.get(0), Some(true));
		assert_eq!(container1.get(1), Some(false));
		assert_eq!(container1.get(2), Some(false));
		assert_eq!(container1.get(3), Some(true));
	}

	#[test]
	fn test_iter() {
		let data = vec![true, false, true];
		let container = BoolContainer::new(data);

		let collected: Vec<Option<bool>> = container.iter().collect();
		assert_eq!(collected, vec![Some(true), Some(false), Some(true)]);
	}

	#[test]
	fn test_slice() {
		let container = BoolContainer::from_vec(vec![true, false, true, false]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(false));
		assert_eq!(sliced.get(1), Some(true));
	}

	#[test]
	fn test_filter() {
		let mut container = BoolContainer::from_vec(vec![true, false, true, false]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(true));
		assert_eq!(container.get(1), Some(true));
	}

	#[test]
	fn test_reorder() {
		let mut container = BoolContainer::from_vec(vec![true, false, true]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(true)); // was index 2
		assert_eq!(container.get(1), Some(true)); // was index 0
		assert_eq!(container.get(2), Some(false)); // was index 1
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let mut container = BoolContainer::from_vec(vec![true, false]);
		let indices = [1, 5, 0]; // index 5 is out of bounds

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(false)); // was index 1
		assert_eq!(container.get(1), Some(false)); // out of bounds -> default (false)
		assert_eq!(container.get(2), Some(true)); // was index 0
	}

	#[test]
	fn testault() {
		let container = BoolContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
