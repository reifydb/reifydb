// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::cowvec::CowVec,
	value::{Value, blob::Blob},
};

pub struct BlobContainer<S: Storage = Cow> {
	data: S::Vec<Blob>,
}

impl<S: Storage> Clone for BlobContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for BlobContainer<S>
where
	S::Vec<Blob>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BlobContainer").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for BlobContainer<S>
where
	S::Vec<Blob>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for BlobContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<Blob>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for BlobContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<Blob>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(BlobContainer {
			data: h.data,
		})
	}
}

impl<S: Storage> Deref for BlobContainer<S> {
	type Target = [Blob];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl BlobContainer<Cow> {
	pub fn new(data: Vec<Blob>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<Blob>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<S: Storage> BlobContainer<S> {
	pub fn from_parts(data: S::Vec<Blob>) -> Self {
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

	pub fn push(&mut self, value: Blob) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, Blob::new(vec![]));
	}

	pub fn get(&self, index: usize) -> Option<&Blob> {
		if index < self.len() {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn data(&self) -> &S::Vec<Blob> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<Blob> {
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
			Value::Blob(self.data[index].clone())
		} else {
			Value::None
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&Blob>> + '_ {
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
				DataVec::push(&mut new_data, Blob::new(vec![]));
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

impl Default for BlobContainer<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_new() {
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![4, 5, 6]);
		let blobs = vec![blob1.clone(), blob2.clone()];
		let container = BlobContainer::new(blobs);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&blob1));
		assert_eq!(container.get(1), Some(&blob2));
	}

	#[test]
	fn test_from_vec() {
		let blob1 = Blob::new(vec![10, 20, 30]);
		let blob2 = Blob::new(vec![40, 50]);
		let blobs = vec![blob1.clone(), blob2.clone()];
		let container = BlobContainer::from_vec(blobs);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&blob1));
		assert_eq!(container.get(1), Some(&blob2));

		for i in 0..2 {
			assert!(container.is_defined(i));
		}
	}

	#[test]
	fn test_with_capacity() {
		let container = BlobContainer::with_capacity(10);
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_push_with_default() {
		let mut container = BlobContainer::with_capacity(3);
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![7, 8, 9]);

		container.push(blob1.clone());
		container.push_default();
		container.push(blob2.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&blob1));
		assert_eq!(container.get(1), Some(&Blob::new(vec![]))); // default
		assert_eq!(container.get(2), Some(&blob2));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_default() {
		let container = BlobContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
