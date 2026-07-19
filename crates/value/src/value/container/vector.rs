// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug},
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	error::{ConstraintKind, Error, TypeError},
	fragment::Fragment,
	reifydb_assertions,
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::{cowvec::CowVec, float_format::format_f32},
	value::{Value, value_type::ValueType, vector::VectorValue},
};

pub struct VectorContainer<S: Storage = Cow> {
	dims: u32,
	data: S::Vec<f32>,
}

impl<S: Storage> Clone for VectorContainer<S> {
	fn clone(&self) -> Self {
		Self {
			dims: self.dims,
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for VectorContainer<S>
where
	S::Vec<f32>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("VectorContainer").field("dims", &self.dims).field("len", &self.len()).finish()
	}
}

impl<S: Storage> PartialEq for VectorContainer<S>
where
	S::Vec<f32>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.dims == other.dims && self.data == other.data
	}
}

impl Serialize for VectorContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			dims: u32,
			data: &'a CowVec<f32>,
		}
		Helper {
			dims: self.dims,
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for VectorContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			dims: u32,
			data: CowVec<f32>,
		}
		let helper = Helper::deserialize(deserializer)?;
		Ok(VectorContainer {
			dims: helper.dims,
			data: helper.data,
		})
	}
}

impl VectorContainer<Cow> {
	pub fn new(dims: u32, data: Vec<f32>) -> Self {
		reifydb_assertions! {
			assert!(dims > 0, "vector dimension must be at least 1");
			assert_eq!(
				data.len() % dims as usize,
				0,
				"vector data length {} is not a multiple of dimension {}",
				data.len(),
				dims
			);
		}
		Self {
			dims,
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(dims: u32, rows: usize) -> Self {
		reifydb_assertions! {
			assert!(dims > 0, "vector dimension must be at least 1");
		}
		Self {
			dims,
			data: CowVec::with_capacity(dims as usize * rows),
		}
	}
}

impl<S: Storage> VectorContainer<S> {
	pub fn dims(&self) -> u32 {
		self.dims
	}

	pub fn len(&self) -> usize {
		DataVec::len(&self.data) / self.dims as usize
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data) / self.dims as usize
	}

	pub fn heap_size(&self) -> usize {
		DataVec::capacity(&self.data) * size_of::<f32>()
	}

	pub fn push(&mut self, value: &[f32]) {
		reifydb_assertions! {
			assert_eq!(
				value.len(),
				self.dims as usize,
				"vector has {} dimensions, column requires {}",
				value.len(),
				self.dims
			);
		}
		DataVec::extend_from_slice(&mut self.data, value);
	}

	pub fn get(&self, index: usize) -> Option<&[f32]> {
		if index >= self.len() {
			return None;
		}
		let dims = self.dims as usize;
		let start = index * dims;
		self.data.as_slice().get(start..start + dims)
	}

	pub fn is_defined(&self, index: usize) -> bool {
		index < self.len()
	}

	pub fn is_fully_defined(&self) -> bool {
		true
	}

	pub fn data(&self) -> &S::Vec<f32> {
		&self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		match self.get(index) {
			Some(values) => {
				let rendered: Vec<String> = values.iter().map(|v| format_f32(*v)).collect();
				format!("[{}]", rendered.join(", "))
			}
			None => "none".to_string(),
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		match self.get(index) {
			Some(values) => Value::Vector(VectorValue::from_slice(values)),
			None => Value::none_of(ValueType::Vector(self.dims)),
		}
	}

	pub fn from_parts(dims: u32, data: S::Vec<f32>) -> Self {
		reifydb_assertions! {
			assert!(dims > 0, "vector dimension must be at least 1");
			assert_eq!(
				DataVec::len(&data) % dims as usize,
				0,
				"vector data length is not a multiple of dimension {}",
				dims
			);
		}
		Self {
			dims,
			data,
		}
	}

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
	}

	pub fn push_default(&mut self) {
		for _ in 0..self.dims {
			DataVec::push(&mut self.data, 0.0f32);
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		if other.dims != self.dims {
			return Err(Error::from(TypeError::ConstraintViolation {
				kind: ConstraintKind::VectorDimension {
					actual: other.dims as usize,
					expected: self.dims as usize,
				},
				message: format!(
					"cannot extend a vector column of {} dimensions with one of {}",
					self.dims, other.dims
				),
				fragment: Fragment::None,
			}));
		}
		DataVec::extend_from_slice(&mut self.data, other.data.as_slice());
		Ok(())
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let dims = self.dims as usize;
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count * dims);
		for i in start..(start + count) {
			DataVec::extend_from_slice(&mut new_data, &self.data.as_slice()[i * dims..(i + 1) * dims]);
		}
		Self {
			dims: self.dims,
			data: new_data,
		}
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let dims = self.dims as usize;
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask) * dims);

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < self.len() {
				DataVec::extend_from_slice(
					&mut new_data,
					&self.data.as_slice()[i * dims..(i + 1) * dims],
				);
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let dims = self.dims as usize;
		let len = self.len();
		let mut new_data = DataVec::spawn(&self.data, indices.len() * dims);

		for &idx in indices {
			if idx < len {
				DataVec::extend_from_slice(
					&mut new_data,
					&self.data.as_slice()[idx * dims..(idx + 1) * dims],
				);
			} else {
				for _ in 0..dims {
					DataVec::push(&mut new_data, 0.0f32);
				}
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		let rows = num.min(self.len());
		Self {
			dims: self.dims,
			data: DataVec::take(&self.data, rows * self.dims as usize),
		}
	}
}

#[cfg(test)]
mod tests {
	use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};

	use super::*;
	use crate::util::bitvec::BitVec;

	#[test]
	fn len_counts_rows_not_floats() {
		let container = VectorContainer::new(4, vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]);
		assert_eq!(container.len(), 2);
		assert_eq!(container.dims(), 4);
	}

	#[test]
	fn get_returns_the_row_slice_not_the_flat_buffer() {
		let container = VectorContainer::new(2, vec![1.0, 2.0, 3.0, 4.0]);
		assert_eq!(container.get(0), Some([1.0f32, 2.0].as_slice()));
		assert_eq!(container.get(1), Some([3.0f32, 4.0].as_slice()));
	}

	#[test]
	fn get_out_of_range_is_none() {
		let container = VectorContainer::new(2, vec![1.0, 2.0]);
		assert_eq!(container.get(1), None);
		assert!(!container.is_defined(1));
	}

	#[test]
	fn push_appends_a_row() {
		let mut container = VectorContainer::with_capacity(3, 2);
		assert!(container.is_empty());
		container.push(&[1.0, 2.0, 3.0]);
		container.push(&[4.0, 5.0, 6.0]);
		assert_eq!(container.len(), 2);
		assert_eq!(container.get(1), Some([4.0f32, 5.0, 6.0].as_slice()));
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "column requires 3")]
	fn push_rejects_a_wrong_length_row() {
		let mut container = VectorContainer::with_capacity(3, 1);
		container.push(&[1.0, 2.0]);
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "not a multiple of dimension")]
	fn new_rejects_ragged_data() {
		VectorContainer::new(3, vec![1.0, 2.0, 3.0, 4.0]);
	}

	#[test]
	fn serde_round_trip_preserves_dims() {
		let original = VectorContainer::new(2, vec![1.0, 2.0, 3.0, 4.0]);
		let encoded: Vec<u8> = postcard_to_allocvec(&original).unwrap();
		let decoded: VectorContainer<Cow> = postcard_from_bytes(&encoded).unwrap();
		assert_eq!(decoded.dims(), 2);
		assert_eq!(decoded.len(), 2);
		assert_eq!(decoded, original);
	}

	#[test]
	fn get_value_returns_a_vector_value() {
		let container = VectorContainer::new(2, vec![1.0, 2.0]);
		assert_eq!(container.get_value(0), Value::vector(vec![1.0, 2.0]));
		assert_eq!(container.get_value(9), Value::none_of(ValueType::Vector(2)));
	}

	#[test]
	fn as_string_renders_the_row() {
		let container = VectorContainer::new(2, vec![0.5, -1.0]);
		assert_eq!(container.as_string(0), "[0.5, -1]");
		assert_eq!(container.as_string(9), "none");
	}

	// The ops below index a flat f32 buffer by row stride. Off-by-one on the stride silently
	// shears every row, so each asserts on whole-row contents, not just row counts.

	fn rows(container: &VectorContainer) -> Vec<Vec<f32>> {
		(0..container.len()).map(|i| container.get(i).unwrap().to_vec()).collect()
	}

	#[test]
	fn slice_extracts_whole_rows() {
		let container = VectorContainer::new(2, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
		let sliced = container.slice(1, 3);
		assert_eq!(sliced.dims(), 2);
		assert_eq!(rows(&sliced), vec![vec![3.0, 4.0], vec![5.0, 6.0]]);
	}

	#[test]
	fn take_keeps_whole_rows() {
		let container = VectorContainer::new(3, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
		let taken = container.take(1);
		assert_eq!(taken.len(), 1);
		assert_eq!(rows(&taken), vec![vec![1.0, 2.0, 3.0]]);
	}

	#[test]
	fn filter_keeps_whole_rows() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
		container.filter(&BitVec::from_slice(&[true, false, true]));
		assert_eq!(rows(&container), vec![vec![1.0, 2.0], vec![5.0, 6.0]]);
	}

	#[test]
	fn reorder_permutes_whole_rows() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
		container.reorder(&[2, 0, 1]);
		assert_eq!(rows(&container), vec![vec![5.0, 6.0], vec![1.0, 2.0], vec![3.0, 4.0]]);
	}

	#[test]
	fn reorder_out_of_range_yields_a_zero_row() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0]);
		container.reorder(&[0, 9]);
		assert_eq!(rows(&container), vec![vec![1.0, 2.0], vec![0.0, 0.0]]);
	}

	#[test]
	fn extend_appends_rows_of_matching_dimension() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0]);
		container.extend(&VectorContainer::new(2, vec![3.0, 4.0])).unwrap();
		assert_eq!(rows(&container), vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
	}

	#[test]
	fn extend_rejects_a_dimension_mismatch() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0]);
		let err = container.extend(&VectorContainer::new(3, vec![1.0, 2.0, 3.0])).unwrap_err();
		assert_eq!(err.0.code, "CONSTRAINT_008");
	}

	#[test]
	fn push_default_appends_a_zero_row() {
		let mut container = VectorContainer::with_capacity(3, 1);
		container.push_default();
		assert_eq!(container.len(), 1);
		assert_eq!(rows(&container), vec![vec![0.0, 0.0, 0.0]]);
	}

	#[test]
	fn clear_drops_every_row_but_keeps_dims() {
		let mut container = VectorContainer::new(2, vec![1.0, 2.0]);
		container.clear();
		assert!(container.is_empty());
		assert_eq!(container.dims(), 2);
	}
}
