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
	value::{Value, blob::Blob},
};

pub struct BlobContainer<S: Storage = Cow> {
	data: S::Vec<Blob>,
	bitvec: S::BitVec,
}

impl<S: Storage> Clone for BlobContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<S: Storage> Debug for BlobContainer<S>
where
	S::Vec<Blob>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BlobContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<S: Storage> PartialEq for BlobContainer<S>
where
	S::Vec<Blob>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl Serialize for BlobContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<Blob>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for BlobContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<Blob>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(BlobContainer {
			data: h.data,
			bitvec: h.bitvec,
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
	pub fn new(data: Vec<Blob>, bitvec: BitVec) -> Self {
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

	pub fn from_vec(data: Vec<Blob>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}
}

impl<S: Storage> BlobContainer<S> {
	pub fn from_parts(data: S::Vec<Blob>, bitvec: S::BitVec) -> Self {
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

	pub fn push(&mut self, value: Blob) {
		DataVec::push(&mut self.data, value);
		DataBitVec::push(&mut self.bitvec, true);
	}

	pub fn push_undefined(&mut self) {
		DataVec::push(&mut self.data, Blob::new(vec![]));
		DataBitVec::push(&mut self.bitvec, false);
	}

	pub fn get(&self, index: usize) -> Option<&Blob> {
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

	pub fn data(&self) -> &S::Vec<Blob> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<Blob> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			"Undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::Blob(self.data[index].clone())
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
			DataVec::push(&mut self.data, Blob::new(vec![]));
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&Blob>> + '_ {
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
				DataVec::push(&mut new_data, Blob::new(vec![]));
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

impl Default for BlobContainer<Cow> {
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
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![4, 5, 6]);
		let blobs = vec![blob1.clone(), blob2.clone()];
		let bitvec = BitVec::from_slice(&[true, true]);
		let container = BlobContainer::new(blobs, bitvec);

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

		// All should be defined
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
	fn test_push_with_undefined() {
		let mut container = BlobContainer::with_capacity(3);
		let blob1 = Blob::new(vec![1, 2, 3]);
		let blob2 = Blob::new(vec![7, 8, 9]);

		container.push(blob1.clone());
		container.push_undefined();
		container.push(blob2.clone());

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&blob1));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), Some(&blob2));

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));
	}

	#[test]
	fn test_extend() {
		let blob1 = Blob::new(vec![1, 2]);
		let blob2 = Blob::new(vec![3, 4]);
		let blob3 = Blob::new(vec![5, 6]);

		let mut container1 = BlobContainer::from_vec(vec![blob1.clone(), blob2.clone()]);
		let container2 = BlobContainer::from_vec(vec![blob3.clone()]);

		container1.extend(&container2).unwrap();

		assert_eq!(container1.len(), 3);
		assert_eq!(container1.get(0), Some(&blob1));
		assert_eq!(container1.get(1), Some(&blob2));
		assert_eq!(container1.get(2), Some(&blob3));
	}

	#[test]
	fn test_extend_from_undefined() {
		let blob = Blob::new(vec![100, 200]);
		let mut container = BlobContainer::from_vec(vec![blob.clone()]);
		container.extend_from_undefined(2);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&blob));
		assert_eq!(container.get(1), None); // undefined
		assert_eq!(container.get(2), None); // undefined
	}

	#[test]
	fn test_iter() {
		let blob1 = Blob::new(vec![1]);
		let blob2 = Blob::new(vec![2]);
		let blob3 = Blob::new(vec![3]);
		let blobs = vec![blob1.clone(), blob2, blob3.clone()];
		let bitvec = BitVec::from_slice(&[true, false, true]); // middle value undefined
		let container = BlobContainer::new(blobs, bitvec);

		let collected: Vec<Option<&Blob>> = container.iter().collect();
		assert_eq!(collected, vec![Some(&blob1), None, Some(&blob3)]);
	}

	#[test]
	fn test_slice() {
		let blobs = vec![Blob::new(vec![1]), Blob::new(vec![2]), Blob::new(vec![3]), Blob::new(vec![4])];
		let container = BlobContainer::from_vec(blobs.clone());
		let sliced = container.slice(1, 3);

		assert_eq!(sliced.len(), 2);
		assert_eq!(sliced.get(0), Some(&blobs[1]));
		assert_eq!(sliced.get(1), Some(&blobs[2]));
	}

	#[test]
	fn test_filter() {
		let blobs = vec![Blob::new(vec![1]), Blob::new(vec![2]), Blob::new(vec![3]), Blob::new(vec![4])];
		let mut container = BlobContainer::from_vec(blobs.clone());
		let mask = BitVec::from_slice(&[true, false, true, false]);

		container.filter(&mask);

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&blobs[0]));
		assert_eq!(container.get(1), Some(&blobs[2]));
	}

	#[test]
	fn test_reorder() {
		let blobs = vec![Blob::new(vec![10]), Blob::new(vec![20]), Blob::new(vec![30])];
		let mut container = BlobContainer::from_vec(blobs.clone());
		let indices = [2, 0, 1];

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&blobs[2])); // was index 2
		assert_eq!(container.get(1), Some(&blobs[0])); // was index 0
		assert_eq!(container.get(2), Some(&blobs[1])); // was index 1
	}

	#[test]
	fn test_reorder_with_out_of_bounds() {
		let blobs = vec![Blob::new(vec![1]), Blob::new(vec![2])];
		let mut container = BlobContainer::from_vec(blobs.clone());
		let indices = [1, 5, 0]; // index 5 is out of bounds

		container.reorder(&indices);

		assert_eq!(container.len(), 3);
		assert_eq!(container.get(0), Some(&blobs[1])); // was index 1
		assert_eq!(container.get(1), None); // out of bounds -> undefined
		assert_eq!(container.get(2), Some(&blobs[0])); // was index 0
	}

	#[test]
	fn test_empty_blobs() {
		let mut container = BlobContainer::with_capacity(2);
		let empty_blob = Blob::new(vec![]);
		let data_blob = Blob::new(vec![1, 2, 3]);

		container.push(empty_blob.clone());
		container.push(data_blob.clone());

		assert_eq!(container.len(), 2);
		assert_eq!(container.get(0), Some(&empty_blob));
		assert_eq!(container.get(1), Some(&data_blob));

		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
	}

	#[test]
	fn test_default() {
		let container = BlobContainer::default();
		assert_eq!(container.len(), 0);
		assert!(container.is_empty());
	}
}
