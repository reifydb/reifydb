// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Result, util::bitvec::BitVec, value::Value};

pub struct AnyContainer {
	data: Vec<Value>,
}

impl Clone for AnyContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl Debug for AnyContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AnyContainer").field("data", &self.data).finish()
	}
}

impl PartialEq for AnyContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for AnyContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a Vec<Value>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for AnyContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: Vec<Value>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(AnyContainer {
			data: h.data,
		})
	}
}

impl Deref for AnyContainer {
	type Target = [Value];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl AnyContainer {
	pub fn new(data: Vec<Value>) -> Self {
		Self {
			data,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<Value>) -> Self {
		Self {
			data,
		}
	}
}

impl AnyContainer {
	pub fn from_parts(data: Vec<Value>) -> Self {
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
		self.capacity() * size_of::<Value>()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: Value) {
		self.data.push(value);
	}

	pub fn push_default(&mut self) {
		self.data.push(Value::none());
	}

	pub fn get(&self, index: usize) -> Option<&Value> {
		self.data.get(index)
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn is_fully_defined(&self) -> bool {
		true
	}

	pub fn data(&self) -> &Vec<Value> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut Vec<Value> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() {
			format!("{}", self.data[index])
		} else {
			"none".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() {
			Value::Any(Box::new(self.data[index].clone()))
		} else {
			Value::none()
		}
	}

	pub fn none_count(&self) -> usize {
		0
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
		}
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
				new_data.push(Value::none());
			}
		}

		self.data = new_data;
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend(other.data.iter().cloned());
		Ok(())
	}
}
