// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug},
	ops::Deref,
	result::Result as StdResult,
};

use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	Result,
	util::shared_vec::SharedVec,
	value::{Value, value_type::ValueType},
};

pub struct AnyContainer {
	data: SharedVec<Value>,
	declared_type: Option<ValueType>,
}

impl Clone for AnyContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			declared_type: self.declared_type.clone(),
		}
	}
}

impl Debug for AnyContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AnyContainer")
			.field("data", &self.data)
			.field("declared_type", &self.declared_type)
			.finish()
	}
}

impl PartialEq for AnyContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.declared_type == other.declared_type
	}
}

impl Serialize for AnyContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a [Value],
			declared_type: &'a Option<ValueType>,
		}
		Helper {
			data: self.data.as_slice(),
			declared_type: &self.declared_type,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for AnyContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: Vec<Value>,
			#[serde(default)]
			declared_type: Option<ValueType>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(AnyContainer {
			data: SharedVec::from_vec(h.data),
			declared_type: h.declared_type,
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
			data: SharedVec::from_vec(data),
			declared_type: None,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: SharedVec::with_capacity(capacity),
			declared_type: None,
		}
	}

	pub fn from_vec(data: Vec<Value>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
			declared_type: None,
		}
	}

	pub fn with_declared_type(mut self, ty: ValueType) -> Self {
		self.declared_type = Some(ty);
		self
	}

	pub fn declared_type(&self) -> Option<&ValueType> {
		self.declared_type.as_ref()
	}
}

impl AnyContainer {
	pub fn from_parts(data: Vec<Value>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
			declared_type: None,
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

	pub fn heap_size(&self) -> usize {
		self.capacity() * size_of::<Value>()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
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

	pub fn data(&self) -> &[Value] {
		self.data.as_slice()
	}

	pub fn data_mut(&mut self) -> &mut Vec<Value> {
		self.data.make_mut()
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
			match &self.declared_type {
				Some(ValueType::List(_)) | Some(ValueType::Record(_)) => self.data[index].clone(),
				_ => Value::Any(Box::new(self.data[index].clone())),
			}
		} else {
			Value::none()
		}
	}

	pub fn none_count(&self) -> usize {
		0
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.slice(0, num),
			declared_type: self.declared_type.clone(),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			data: self.data.slice(start, end),
			declared_type: self.declared_type.clone(),
		}
	}

	pub fn filter(&mut self, mask: &BooleanBuffer) {
		let mut new_data = Vec::with_capacity(mask.count_set_bits());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
			}
		}

		self.data = SharedVec::from_vec(new_data);
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

		self.data = SharedVec::from_vec(new_data);
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.make_mut().extend(other.data.iter().cloned());
		Ok(())
	}
}
