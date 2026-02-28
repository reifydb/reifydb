// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::cowvec::CowVec,
	value::{Value, r#type::Type},
};

pub struct Utf8Container<S: Storage = Cow> {
	data: S::Vec<String>,
}

impl<S: Storage> Clone for Utf8Container<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for Utf8Container<S>
where
	S::Vec<String>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Utf8Container").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for Utf8Container<S>
where
	S::Vec<String>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for Utf8Container<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> std::result::Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<String>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Utf8Container<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<String>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(Utf8Container {
			data: h.data,
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
	pub fn new(data: Vec<String>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	/// Reconstruct from raw parts previously obtained via `try_into_raw_parts`.
	pub fn from_raw_parts(data: Vec<String>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	/// Try to decompose into raw Vec for recycling.
	/// Returns `None` if the inner storage is shared.
	pub fn try_into_raw_parts(self) -> Option<Vec<String>> {
		match self.data.try_into_vec() {
			Ok(v) => Some(v),
			Err(_) => None,
		}
	}

	pub fn from_vec(data: Vec<String>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<S: Storage> Utf8Container<S> {
	pub fn from_parts(data: S::Vec<String>) -> Self {
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

	pub fn push(&mut self, value: String) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, String::new());
	}

	pub fn get(&self, index: usize) -> Option<&String> {
		if index < self.len() {
			DataVec::get(&self.data, index)
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

	pub fn data(&self) -> &S::Vec<String> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<String> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			self.data[index].clone()
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			Value::Utf8(self.data[index].clone())
		} else {
			Value::none_of(Type::Utf8)
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&String>> + '_ {
		self.data.iter().map(|v| Some(v))
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
				DataVec::push(&mut new_data, String::new());
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
		let container = Utf8Container::new(data.clone());

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
		container.push_default();

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&"first".to_string()));
		assert_eq!(container.get(1), Some(&"second".to_string()));
		assert_eq!(container.get(2), Some(&"".to_string())); // push_default pushes default

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
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
	fn test_iter() {
		let data = vec!["x".to_string(), "y".to_string(), "z".to_string()];
		let container = Utf8Container::new(data);

		let collected: Vec<Option<&String>> = container.iter().collect();
		assert_eq!(collected, vec![Some(&"x".to_string()), Some(&"y".to_string()), Some(&"z".to_string())]);
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
		assert_eq!(container.get(1), Some(&"".to_string())); // out of bounds -> default
		assert_eq!(container.get(2), Some(&"a".to_string())); // was index 0
	}

	#[test]
	fn test_empty_strings() {
		let mut container = Utf8Container::with_capacity(2);
		container.push("".to_string()); // empty string
		container.push_default();

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&"".to_string()));
		assert_eq!(container.get(1), Some(&"".to_string()));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
	}

	#[test]
	fn test_default() {
		let container = Utf8Container::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
