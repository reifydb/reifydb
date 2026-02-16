// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	any::TypeId,
	fmt::{self, Debug},
	mem::transmute_copy,
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::cowvec::CowVec,
	value::{
		Value,
		is::IsUuid,
		uuid::{Uuid4, Uuid7},
	},
};

pub struct UuidContainer<T, S: Storage = Cow>
where
	T: IsUuid,
{
	data: S::Vec<T>,
}

impl<T: IsUuid, S: Storage> Clone for UuidContainer<T, S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<T: IsUuid + Debug, S: Storage> Debug for UuidContainer<T, S>
where
	S::Vec<T>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("UuidContainer").field("data", &self.data).finish()
	}
}

impl<T: IsUuid, S: Storage> PartialEq for UuidContainer<T, S>
where
	S::Vec<T>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl<T: IsUuid + Serialize> Serialize for UuidContainer<T, Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a CowVec<T>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsUuid + Deserialize<'de>> Deserialize<'de> for UuidContainer<T, Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: CowVec<T>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(UuidContainer {
			data: h.data,
		})
	}
}

impl<T: IsUuid, S: Storage> Deref for UuidContainer<T, S> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl<T> UuidContainer<T, Cow>
where
	T: IsUuid + Clone + Debug + Default,
{
	pub fn new(data: Vec<T>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<T>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<T, S: Storage> UuidContainer<T, S>
where
	T: IsUuid + Clone + Debug + Default,
{
	pub fn from_parts(data: S::Vec<T>) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		DataVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data)
	}

	pub fn is_empty(&self) -> bool {
		DataVec::is_empty(&self.data)
	}

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
	}

	pub fn push(&mut self, value: T) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, T::default());
	}

	pub fn get(&self, index: usize) -> Option<&T> {
		if index < self.len() {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn data(&self) -> &S::Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value
	where
		T: 'static,
	{
		if index < self.len() {
			let value = self.data[index].clone();

			if TypeId::of::<T>() == TypeId::of::<Uuid4>() {
				let uuid_val = unsafe { transmute_copy::<T, Uuid4>(&value) };
				Value::Uuid4(uuid_val)
			} else if TypeId::of::<T>() == TypeId::of::<Uuid7>() {
				let uuid_val = unsafe { transmute_copy::<T, Uuid7>(&value) };
				Value::Uuid7(uuid_val)
			} else {
				Value::none()
			}
		} else {
			Value::none()
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
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
		let mut new_data = DataVec::spawn(&self.data, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i].clone());
		}
		Self {
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataVec::push(&mut new_data, self.data[i].clone());
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataVec::push(&mut new_data, self.data[idx].clone());
			} else {
				DataVec::push(&mut new_data, T::default());
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
		}
	}
}

impl<T> Default for UuidContainer<T, Cow>
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
		let uuid1 = Uuid7::generate();
		let uuid2 = Uuid7::generate();
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
	fn test_default() {
		let container: UuidContainer<Uuid4> = UuidContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
