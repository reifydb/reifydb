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
	util::{bitvec::BitVec, cowvec::CowVec},
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
	bitvec: S::BitVec,
}

impl<T: IsUuid, S: Storage> Clone for UuidContainer<T, S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<T: IsUuid + Debug, S: Storage> Debug for UuidContainer<T, S>
where
	S::Vec<T>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("UuidContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<T: IsUuid, S: Storage> PartialEq for UuidContainer<T, S>
where
	S::Vec<T>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl<T: IsUuid + Serialize> Serialize for UuidContainer<T, Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a, T: Clone + PartialEq + Serialize> {
			data: &'a CowVec<T>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de, T: IsUuid + Deserialize<'de>> Deserialize<'de> for UuidContainer<T, Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper<T: Clone + PartialEq> {
			data: CowVec<T>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(UuidContainer {
			data: h.data,
			bitvec: h.bitvec,
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
	pub fn new(data: Vec<T>, bitvec: BitVec) -> Self {
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

	pub fn from_vec(data: Vec<T>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}
}

impl<T, S: Storage> UuidContainer<T, S>
where
	T: IsUuid + Clone + Debug + Default,
{
	pub fn from_parts(data: S::Vec<T>, bitvec: S::BitVec) -> Self {
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

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
		DataBitVec::clear(&mut self.bitvec);
	}

	pub fn push(&mut self, value: T) {
		DataVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataVec::push(&mut self.data, T::default());
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<&T> {
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

	pub fn data(&self) -> &S::Vec<T> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<T> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value
	where
		T: 'static,
	{
		if index < self.len() && self.is_defined(index) {
			let value = self.data[index].clone();

			if TypeId::of::<T>() == TypeId::of::<Uuid4>() {
				let uuid_val = unsafe { transmute_copy::<T, Uuid4>(&value) };
				Value::Uuid4(uuid_val)
			} else if TypeId::of::<T>() == TypeId::of::<Uuid7>() {
				let uuid_val = unsafe { transmute_copy::<T, Uuid7>(&value) };
				Value::Uuid7(uuid_val)
			} else {
				Value::None
			}
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
			DataVec::push(&mut self.data, T::default());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<T>> + '_
	where
		T: Copy,
	{
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
				DataVec::push(&mut new_data, T::default());
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
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_uuid4_container() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let uuids = vec![uuid1, uuid2];
		let container = UuidContainer::from_vec(uuids.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&uuids[0]));
		assert_eq!(container.get(1), Some(&uuids[1]));

		// All should be defined
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
	fn test_push_with_undefined() {
		let mut container: UuidContainer<Uuid4> = UuidContainer::with_capacity(3);
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();

		container.push(uuid1);
		container.push_undefined();
		container.push(uuid2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&uuid1));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), Some(&uuid2));

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let uuid3 = Uuid4::generate();

		let mut container1 = UuidContainer::from_vec(vec![uuid1, uuid2]);
		let container2 = UuidContainer::from_vec(vec![uuid3]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 3);
		assert_eq!(container1.get(0), Some(&uuid1));
		assert_eq!(container1.get(1), Some(&uuid2));
		assert_eq!(container1.get(2), Some(&uuid3));
	}

	#[test]
	fn test_extend_from_undefined() {
		let uuid = Uuid7::generate();
		let mut container = UuidContainer::from_vec(vec![uuid]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&uuid));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), None); // undefined
	}

	#[test]
	fn test_iter() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let uuid3 = Uuid4::generate();
		let uuids = vec![uuid1, uuid2, uuid3];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = UuidContainer::new(uuids.clone(), bitvec);

		let collected: Vec<Option<Uuid4>> = container.iter().collect();
		assert_eq!(collected, vec![Some(uuids[0]), None, Some(uuids[2])]);
	}

	#[test]
	fn test_slice() {
		let uuids = vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
		let container = UuidContainer::from_vec(uuids.clone());
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&uuids[1]));
		assert_eq!(sliced.get(1), Some(&uuids[2]));
	}

	#[test]
	fn test_filter() {
		let uuids = vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
		let mut container = UuidContainer::from_vec(uuids.clone());
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&uuids[0]));
		assert_eq!(container.get(1), Some(&uuids[2]));
	}

	#[test]
	fn test_reorder() {
		let uuids = vec![Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
		let mut container = UuidContainer::from_vec(uuids.clone());
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&uuids[2])); // was index 2
		assert_eq!(container.get(1), Some(&uuids[0])); // was index 0
		assert_eq!(container.get(2), Some(&uuids[1])); // was index 1
	}

	#[test]
	fn test_mixed_uuid_types() {
		// Test that we can have different UUID containers
		let uuid4_container: UuidContainer<Uuid4> = UuidContainer::from_vec(vec![Uuid4::generate()]);
		let uuid7_container: UuidContainer<Uuid7> = UuidContainer::from_vec(vec![Uuid7::generate()]);

		assert_eq!(uuid4_container.len(), 1);
		assert_eq!(uuid7_container.len(), 1);
	}

	#[test]
	fn test_default() {
		let container: UuidContainer<Uuid4> = UuidContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
