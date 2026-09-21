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
	util::{bitvec::BitVec, shared_vec::SharedVec},
	value::{Value, row_number::RowNumber, value_type::ValueType},
};

pub struct RowNumberContainer {
	data: SharedVec<RowNumber>,
}

impl Clone for RowNumberContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl Debug for RowNumberContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RowNumberContainer").field("data", &self.data).finish()
	}
}

impl PartialEq for RowNumberContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for RowNumberContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a [RowNumber],
		}

		Helper {
			data: self.data.as_slice(),
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for RowNumberContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: Vec<RowNumber>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(RowNumberContainer {
			data: SharedVec::from_vec(h.data),
		})
	}
}

impl Deref for RowNumberContainer {
	type Target = [RowNumber];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl RowNumberContainer {
	pub fn new(data: Vec<RowNumber>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: SharedVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<RowNumber>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
		}
	}
}

impl RowNumberContainer {
	pub fn from_parts(data: Vec<RowNumber>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn freeze(&mut self) {
		self.data.freeze();
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn push(&mut self, value: RowNumber) {
		self.data.push(value);
	}

	pub fn push_default(&mut self) {
		self.data.push(RowNumber::default());
	}

	pub fn get(&self, index: usize) -> Option<&RowNumber> {
		if index < self.len() {
			self.data.get(index)
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn data(&self) -> &[RowNumber] {
		self.data.as_slice()
	}

	pub fn data_mut(&mut self) -> &mut Vec<RowNumber> {
		self.data.make_mut()
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
			Value::Uint8(self.data[index].value())
		} else {
			Value::none_of(ValueType::Uint8)
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.make_mut().extend(other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<RowNumber>> + '_ {
		self.data.iter().map(|&v| Some(v))
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			data: self.data.slice(start, end),
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i]);
			}
		}

		self.data = SharedVec::from_vec(new_data);
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx]);
			} else {
				new_data.push(RowNumber::default());
			}
		}

		self.data = SharedVec::from_vec(new_data);
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.slice(0, num),
		}
	}
}

impl Default for RowNumberContainer {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}
