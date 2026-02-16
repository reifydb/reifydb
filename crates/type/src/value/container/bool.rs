// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, Storage},
	util::bitvec::BitVec,
	value::Value,
};

pub struct BoolContainer<S: Storage = Cow> {
	data: S::BitVec,
}

impl<S: Storage> Clone for BoolContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for BoolContainer<S>
where
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BoolContainer").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for BoolContainer<S>
where
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for BoolContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
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

impl<'de> Deserialize<'de> for BoolContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
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

impl Deref for BoolContainer<Cow> {
	type Target = BitVec;

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl BoolContainer<Cow> {
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

impl<S: Storage> BoolContainer<S> {
	pub fn from_parts(data: S::BitVec) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		DataBitVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataBitVec::capacity(&self.data)
	}

	pub fn is_empty(&self) -> bool {
		DataBitVec::len(&self.data) == 0
	}

	pub fn clear(&mut self) {
		DataBitVec::clear(&mut self.data);
	}

	pub fn push(&mut self, value: bool) {
		DataBitVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataBitVec::push(&mut self.data, false);
	}

	pub fn get(&self, index: usize) -> Option<bool> {
		if index < self.len() {
			Some(DataBitVec::get(&self.data, index))
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

	pub fn data(&self) -> &S::BitVec {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::BitVec {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			DataBitVec::get(&self.data, index).to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			Value::Boolean(DataBitVec::get(&self.data, index))
		} else {
			Value::None
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataBitVec::extend_from(&mut self.data, &other.data);
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
		DataBitVec::iter(&self.data).map(|v| Some(v))
	}

	pub fn into_iter(self) -> impl Iterator<Item = Option<bool>> {
		let data: Vec<bool> = DataBitVec::iter(&self.data).collect();
		data.into_iter().map(|v| Some(v))
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataBitVec::spawn(&self.data, count);
		for i in start..(start + count) {
			DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, i));
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataBitVec::spawn(&self.data, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, i));
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataBitVec::spawn(&self.data, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, idx));
			} else {
				DataBitVec::push(&mut new_data, false);
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataBitVec::take(&self.data, num),
		}
	}
}

impl Default for BoolContainer<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

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

		// All should be defined
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
		use crate::util::bitvec::BitVec;
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
	fn test_default() {
		let container = BoolContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
