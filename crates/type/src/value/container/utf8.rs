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
	value::Value,
};

pub struct Utf8Container<S: Storage = Cow> {
	data: S::Vec<String>,
	bitvec: S::BitVec,
}

impl<S: Storage> Clone for Utf8Container<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<S: Storage> Debug for Utf8Container<S>
where
	S::Vec<String>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Utf8Container").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<S: Storage> PartialEq for Utf8Container<S>
where
	S::Vec<String>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl Serialize for Utf8Container<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<String>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Utf8Container<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<String>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(Utf8Container {
			data: h.data,
			bitvec: h.bitvec,
		})
	}
}

impl<S: Storage> Deref for Utf8Container<S> {
	type Target = [String];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl Utf8Container<Cow> {
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

	/// Reconstruct from raw parts previously obtained via `try_into_raw_parts`.
	pub fn from_raw_parts(data: Vec<String>, bitvec_bits: Vec<u8>, bitvec_len: usize) -> Self {
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::from_raw(bitvec_bits, bitvec_len),
		}
	}

	/// Try to decompose into raw Vec + bitvec bytes for recycling.
	/// Returns `None` if the inner storage is shared.
	pub fn try_into_raw_parts(self) -> Option<(Vec<String>, Vec<u8>, usize)> {
		let data = match self.data.try_into_vec() {
			Ok(v) => v,
			Err(_) => return None,
		};
		match self.bitvec.try_into_raw() {
			Ok((bits, len)) => Some((data, bits, len)),
			Err(_) => None,
		}
	}

	pub fn from_vec(data: Vec<String>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}
}

impl<S: Storage> Utf8Container<S> {
	pub fn from_parts(data: S::Vec<String>, bitvec: S::BitVec) -> Self {
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

	pub fn push(&mut self, value: String) {
		DataVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataVec::push(&mut self.data, String::new());
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<&String> {
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

	pub fn is_fully_defined(&self) -> bool {
		DataBitVec::count_ones(&self.bitvec) == self.len()
	}

	pub fn data(&self) -> &S::Vec<String> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<String> {
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
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		DataBitVec::extend_from(&mut self.bitvec, &other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		for _ in 0..len {
			DataVec::push(&mut self.data, String::new());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&String>> + '_ {
		self.data.iter().zip(DataBitVec::iter(&self.bitvec)).map(|(v, defined)| {
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
				DataVec::push(&mut new_data, String::new());
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

impl Default for Utf8Container<Cow> {
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
