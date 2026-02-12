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
	bitvec: S::BitVec,
}

impl<S: Storage> Clone for BoolContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<S: Storage> Debug for BoolContainer<S>
where
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BoolContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<S: Storage> PartialEq for BoolContainer<S>
where
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl Serialize for BoolContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a BitVec,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for BoolContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: BitVec,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(BoolContainer {
			data: h.data,
			bitvec: h.bitvec,
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
	pub fn new(data: Vec<bool>, bitvec: BitVec) -> Self {
		debug_assert_eq!(data.len(), bitvec.len());
		Self {
			data: BitVec::from_slice(&data),
			bitvec,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: BitVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
		}
	}

	/// Reconstruct from raw bitvec bytes previously obtained via `try_into_raw_parts`.
	pub fn from_raw_parts(data_bits: Vec<u8>, data_len: usize, bitvec_bits: Vec<u8>, bitvec_len: usize) -> Self {
		Self {
			data: BitVec::from_raw(data_bits, data_len),
			bitvec: BitVec::from_raw(bitvec_bits, bitvec_len),
		}
	}

	/// Try to decompose into raw bitvec bytes for recycling.
	/// Returns `None` if the inner storage is shared.
	pub fn try_into_raw_parts(self) -> Option<(Vec<u8>, usize, Vec<u8>, usize)> {
		let (data_bits, data_len) = match self.data.try_into_raw() {
			Ok(v) => v,
			Err(_) => return None,
		};
		match self.bitvec.try_into_raw() {
			Ok((bv_bits, bv_len)) => Some((data_bits, data_len, bv_bits, bv_len)),
			Err(_) => None,
		}
	}

	pub fn from_vec(data: Vec<bool>) -> Self {
		let len = data.len();
		Self {
			data: BitVec::from_slice(&data),
			bitvec: BitVec::repeat(len, true),
		}
	}
}

impl<S: Storage> BoolContainer<S> {
	pub fn from_parts(data: S::BitVec, bitvec: S::BitVec) -> Self {
		Self {
			data,
			bitvec,
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(DataBitVec::len(&self.data), DataBitVec::len(&self.bitvec));
		DataBitVec::len(&self.data)
	}

	pub fn capacity(&self) -> usize {
		DataBitVec::capacity(&self.data).min(DataBitVec::capacity(&self.bitvec))
	}

	pub fn is_empty(&self) -> bool {
		DataBitVec::len(&self.data) == 0
	}

	pub fn clear(&mut self) {
		DataBitVec::clear(&mut self.data);
		DataBitVec::clear(&mut self.bitvec);
	}

	pub fn push(&mut self, value: bool) {
		DataBitVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataBitVec::push(&mut self.data, false);
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<bool> {
		if index < self.len() && self.is_defined(index) {
			Some(DataBitVec::get(&self.data, index))
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

	pub fn is_fully_defined(&self) -> bool {
		DataBitVec::count_ones(&self.bitvec) == self.len()
	}

	pub fn data(&self) -> &S::BitVec {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::BitVec {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			DataBitVec::get(&self.data, index).to_string()
		} else {
			"Undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::Boolean(DataBitVec::get(&self.data, index))
		} else {
			Value::Undefined
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataBitVec::extend_from(&mut self.data, &other.data);
		DataBitVec::extend_from(&mut self.bitvec, &other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		for _ in 0..len {
			DataBitVec::push(&mut self.data, false);
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
		DataBitVec::iter(&self.data).zip(DataBitVec::iter(&self.bitvec)).map(|(v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn into_iter(self) -> impl Iterator<Item = Option<bool>> {
		let data: Vec<bool> = DataBitVec::iter(&self.data).collect();
		let bitvec: Vec<bool> = DataBitVec::iter(&self.bitvec).collect();
		data.into_iter().zip(bitvec).map(|(v, defined)| {
			if defined {
				Some(v)
			} else {
				None
			}
		})
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataBitVec::spawn(&self.data, count);
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, count);
		for i in start..(start + count) {
			DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, i));
			DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
		}
		Self {
			data: new_data,
			bitvec: new_bitvec,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataBitVec::spawn(&self.data, DataBitVec::count_ones(mask));
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, i));
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataBitVec::spawn(&self.data, indices.len());
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, indices.len());

		for &idx in indices {
			if idx < self.len() {
				DataBitVec::push(&mut new_data, DataBitVec::get(&self.data, idx));
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, idx));
			} else {
				DataBitVec::push(&mut new_data, false);
				DataBitVec::push(&mut new_bitvec, false);
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataBitVec::take(&self.data, num),
			bitvec: DataBitVec::take(&self.bitvec, num),
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
	use crate::util::bitvec::BitVec;

	#[test]
	fn test_new() {
		let data = vec![true, false, true];
		let bitvec = BitVec::from_slice(&[true, true, true]);
		let container = BoolContainer::new(data.clone(), bitvec);

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
		container.push_undefined();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(true));
		assert_eq!(container.get(1), Some(false));
		assert_eq!(container.get(2), None); // undefined

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(!container.is_defined(2));
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
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = BoolContainer::new(data, bitvec);

		let collected: Vec<Option<bool>> = container.iter().collect();
		assert_eq!(collected, vec![Some(true), None, Some(true)]);
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
		assert_eq!(container.get(1), None); // out of bounds -> undefined
		assert_eq!(container.get(2), Some(true)); // was index 0
	}

	#[test]
	fn test_default() {
		let container = BoolContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
