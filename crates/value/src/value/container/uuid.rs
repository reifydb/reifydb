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
	value::{Value, is::IsUuid},
};

pub struct UuidContainer<T>
where
	T: IsUuid,
{
	data: Vec<T>,
}

impl<T: IsUuid> Clone for UuidContainer<T> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsUuid + Debug> Debug for UuidContainer<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("UuidContainer").field("data", &self.data).finish()
	}
}

impl<T: IsUuid> PartialEq for UuidContainer<T> {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsUuid + Serialize> Serialize for UuidContainer<T> {
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

impl<'de, T: IsUuid + Deserialize<'de>> Deserialize<'de> for UuidContainer<T> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: Vec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(UuidContainer {
			data: h.data,
		})
	}
}

impl<T: IsUuid> Deref for UuidContainer<T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> UuidContainer<T>
where
	T: IsUuid + Clone + Debug + Default,
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

impl<T> UuidContainer<T>
where
	T: IsUuid + Clone + Debug + Default,
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

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
		}
	}
}

impl<T> Default for UuidContainer<T>
where
	T: IsUuid + Clone + Debug + Default,
{
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		clock::testing::{TestClock, TestRng},
		value::uuid::{Uuid4, Uuid7},
	};

	fn test_clock_and_rng() -> (TestClock, TestClock, TestRng) {
		let clock = TestClock::from_millis(1000);
		(clock.clone(), clock, TestRng)
	}

	#[test]
	fn test_uuid4_container() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let uuids = vec![uuid1, uuid2];
		let container = UuidContainer::from_vec(uuids.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&uuids[0]));
		assert_eq!(container.get(1), Some(&uuids[1]));

		for i in 0..2 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_uuid7_container() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid2 = Uuid7::generate(&clock, &rng);
		let uuids = vec![uuid1, uuid2];
		let container = UuidContainer::from_vec(uuids.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&uuids[0]));
		assert_eq!(container.get(1), Some(&uuids[1]));
	}

	#[test]
	fn test_with_capacity() {
		let container: UuidContainer<Uuid4> = UuidContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_with_default() {
		let mut container: UuidContainer<Uuid4> = UuidContainer::with_capacity(3);
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();

		container.push(uuid1);
		container.push_default();
		container.push(uuid2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&uuid1));
		assert_eq!(container.get(1), Some(&Uuid4::default())); // default
		assert_eq!(container.get(2), Some(&uuid2));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn testault() {
		let container: UuidContainer<Uuid4> = UuidContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
