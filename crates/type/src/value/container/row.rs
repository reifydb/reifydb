// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{Value, row_number::RowNumber},
};

pub struct RowNumberContainer<S: Storage = Cow> {
	data: S::Vec<RowNumber>,
	bitvec: S::BitVec,
}

impl<S: Storage> Clone for RowNumberContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<S: Storage> Debug for RowNumberContainer<S>
where
	S::Vec<RowNumber>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RowNumberContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<S: Storage> PartialEq for RowNumberContainer<S>
where
	S::Vec<RowNumber>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl Serialize for RowNumberContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<RowNumber>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for RowNumberContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<RowNumber>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(RowNumberContainer {
			data: h.data,
			bitvec: h.bitvec,
		})
	}
}

impl<S: Storage> Deref for RowNumberContainer<S> {
	type Target = [RowNumber];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl RowNumberContainer<Cow> {
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
}

impl<S: Storage> RowNumberContainer<S> {
	pub fn from_parts(data: S::Vec<RowNumber>, bitvec: S::BitVec) -> Self {
		Self {
			data,
			bitvec,
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(DataVec::len(&self.data), DataBitVec::len(&self.bitvec));
		DataVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data).min(DataBitVec::capacity(&self.bitvec))
	}

	pub fn is_empty(&self) -> bool {
		DataVec::is_empty(&self.data)
	}

	pub fn push(&mut self, value: RowNumber) {
		DataVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataVec::push(&mut self.data, RowNumber::default());
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<&RowNumber> {
		if index < self.len() && self.is_defined(index) {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn bitvec(&self) -> &S::BitVec {
		&self.bitvec
	}

	pub fn bitvec_mut(&mut self) -> &mut S::BitVec {
		&mut self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && DataBitVec::get(&self.bitvec, idx)
	}

	pub fn data(&self) -> &S::Vec<RowNumber> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<RowNumber> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::Uint8(self.data[index].value())
		} else {
			Value::None
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		DataBitVec::extend_from(&mut self.bitvec, &other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		for _ in 0..len {
			DataVec::push(&mut self.data, RowNumber::default());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<RowNumber>> + '_ {
		self.data.iter().zip(DataBitVec::iter(&self.bitvec)).map(|(&v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count);
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i].clone());
			DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
		}
		Self {
			data: new_data,
			bitvec: new_bitvec,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataVec::push(&mut new_data, self.data[i].clone());
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataVec::push(&mut new_data, self.data[idx].clone());
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, idx));
			} else {
				DataVec::push(&mut new_data, RowNumber::default());
				DataBitVec::push(&mut new_bitvec, false);
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
			bitvec: DataBitVec::take(&self.bitvec, num),
		}
	}
}

impl Default for RowNumberContainer<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_new() {
		let row_number1 = RowNumber::new(1);
		let row_number2 = RowNumber::new(2);
		let row_numbers = vec![row_number1, row_number2];
		let bitvec = BitVec::from_slice(&[true, true]);
		let container = RowNumberContainer::new(row_numbers.clone(), bitvec);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&row_numbers[0]));
		assert_eq!(container.get(1), Some(&row_numbers[1]));
	}

	#[test]
	fn test_from_vec() {
		let row_numbers = vec![RowNumber::new(10), RowNumber::new(20), RowNumber::new(30)];
		let container = RowNumberContainer::from_vec(row_numbers.clone());

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

		let mut container1 = RowNumberContainer::from_vec(vec![row_number1, row_number2]);
		let container2 = RowNumberContainer::from_vec(vec![row_number3]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 3);
		assert_eq!(container1.get(0), Some(&row_number1));
		assert_eq!(container1.get(1), Some(&row_number2));
		assert_eq!(container1.get(2), Some(&row_number3));
	}

	#[test]
	fn test_extend_from_undefined() {
		let row_number = RowNumber::new(42);
		let mut container = RowNumberContainer::from_vec(vec![row_number]);
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
		let container = RowNumberContainer::new(row_numbers.clone(), bitvec);

		let collected: Vec<Option<RowNumber>> = container.iter().collect();
		assert_eq!(collected, vec![Some(row_numbers[0]), None, Some(row_numbers[2])]);
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
		let mut container =
			RowNumberContainer::from_vec(vec![RowNumber::new(10), RowNumber::new(20), RowNumber::new(30)]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&RowNumber::new(30))); // was index 2
		assert_eq!(container.get(1), Some(&RowNumber::new(10))); // was index 0
		assert_eq!(container.get(2), Some(&RowNumber::new(20))); // was index 1
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let mut container = RowNumberContainer::from_vec(vec![RowNumber::new(1), RowNumber::new(2)]);
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
		let mut container = RowNumberContainer::from_vec(vec![RowNumber::new(1), RowNumber::new(2)]);

		// Test immutable access
		assert_eq!(container.data().len(), 2);

		// Test mutable access
		container.data_mut().push(RowNumber::new(3));
		container.bitvec_mut().push(true);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(2), Some(&RowNumber::new(3)));
	}
}
