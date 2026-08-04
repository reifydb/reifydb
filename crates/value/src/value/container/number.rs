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
	value::{Value, is::IsNumber},
};

pub struct NumberContainer<T>
where
	T: IsNumber,
{
	data: Vec<T>,
}

impl<T: IsNumber> Clone for NumberContainer<T> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsNumber + Debug> Debug for NumberContainer<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("NumberContainer").field("data", &self.data).finish()
	}
}

impl<T: IsNumber> PartialEq for NumberContainer<T> {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsNumber + Serialize> Serialize for NumberContainer<T> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a Vec<T>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsNumber + Deserialize<'de>> Deserialize<'de> for NumberContainer<T> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: Vec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(NumberContainer {
			data: h.data,
		})
	}
}

impl<T: IsNumber> Deref for NumberContainer<T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> NumberContainer<T>
where
	T: IsNumber + Clone + Debug + Default,
{
	pub fn new(data: Vec<T>) -> Self {
		Self {
			data,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<T>) -> Self {
		Self {
			data,
		}
	}
}

impl<T> NumberContainer<T>
where
	T: IsNumber + Clone + Debug + Default,
{
	pub fn from_parts(data: Vec<T>) -> Self {
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
		self.capacity() * size_of::<T>()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: T) {
		self.data.push(value);
	}

	pub fn push_default(&mut self) {
		self.data.push(T::default());
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() {
			self.data.get(index)
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

	pub fn data(&self) -> &Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut Vec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			self.data[index].to_value()
		} else {
			Value::none()
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend(other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
	where
		T: Copy,
	{
		self.data.iter().map(|&v| Some(v))
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = Vec::with_capacity(count);
		for i in start..(start + count) {
			new_data.push(self.data[i].clone());
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx].clone());
			} else {
				new_data.push(T::default());
			}
		}

		self.data = new_data;
	}

	pub fn push_with_convert<U>(&mut self, value: U, converter: impl FnOnce(U) -> Option<T>) {
		match converter(value) {
			Some(v) => {
				self.data.push(v);
			}
			None => {
				self.data.push(T::default());
			}
		}
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_new_i32() {
		let data = vec![1, 2, 3];
		let container = NumberContainer::new(data.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&1));
		assert_eq!(container.get(1), Some(&2));
		assert_eq!(container.get(2), Some(&3));
	}

	#[test]
	fn test_from_vec_f64() {
		let data = vec![1.1, 2.2, 3.3];
		let container = NumberContainer::from_vec(data);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&1.1));
		assert_eq!(container.get(1), Some(&2.2));
		assert_eq!(container.get(2), Some(&3.3));

		for i in 0..3 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container: NumberContainer<i32> = NumberContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_i64() {
		let mut container: NumberContainer<i64> = NumberContainer::with_capacity(3);

		container.push(100);
		container.push(-200);
		container.push_default();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&100));
		assert_eq!(container.get(1), Some(&-200));
		assert_eq!(container.get(2), Some(&0)); // push_default pushes default

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let mut container1 = NumberContainer::from_vec(vec![1i32, 2]);
		let container2 = NumberContainer::from_vec(vec![3i32, 4]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 4);
		assert_eq!(container1.get(0), Some(&1));
		assert_eq!(container1.get(1), Some(&2));
		assert_eq!(container1.get(2), Some(&3));
		assert_eq!(container1.get(3), Some(&4));
	}

	#[test]
	fn test_iter_u8() {
		let data = vec![1u8, 2, 3];
		let container = NumberContainer::new(data);

		let collected: Vec<Option<u8>> = container.iter().collect();
		assert_eq!(collected, vec![Some(1), Some(2), Some(3)]);
	}

	#[test]
	fn test_slice() {
		let container = NumberContainer::from_vec(vec![10i16, 20, 30, 40]);
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&20));
		assert_eq!(sliced.get(1), Some(&30));
	}

	#[test]
	fn test_filter() {
		let mut container = NumberContainer::from_vec(vec![1f32, 2.0, 3.0, 4.0]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&1.0));
		assert_eq!(container.get(1), Some(&3.0));
	}

	#[test]
	fn test_reorder() {
		let mut container = NumberContainer::from_vec(vec![10i32, 20, 30]);
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&30)); // was index 2
		assert_eq!(container.get(1), Some(&10)); // was index 0
		assert_eq!(container.get(2), Some(&20)); // was index 1
	}

	#[test]
	fn test_push_with_convert() {
		let mut container: NumberContainer<i32> = NumberContainer::with_capacity(3);

		// A failed conversion still pushes, keeping the row count aligned with the other
		// columns; it lands on the default rather than being skipped.
		container.push_with_convert(42u32, |x| {
			if x <= i32::MAX as u32 {
				Some(x as i32)
			} else {
				None
			}
		});

		container.push_with_convert(u32::MAX, |x| {
			if x <= i32::MAX as u32 {
				Some(x as i32)
			} else {
				None
			}
		});

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&42));
		assert_eq!(container.get(1), Some(&0)); // conversion failed, pushed default

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
	}

	#[test]
	fn test_data_access() {
		let mut container = NumberContainer::from_vec(vec![1i32, 2, 3]);

		assert_eq!(container.data().len(), 3);

		// Pushing through data_mut must keep len() in step, since it bypasses push().
		container.data_mut().push(4);

		assert_eq!(container.len(), 4);
		assert_eq!(container.get(3), Some(&4));
	}
}
