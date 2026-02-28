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
	value::Value,
};

pub struct AnyContainer<S: Storage = Cow> {
	data: S::Vec<Box<Value>>,
}

impl<S: Storage> Clone for AnyContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for AnyContainer<S>
where
	S::Vec<Box<Value>>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AnyContainer").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for AnyContainer<S>
where
	S::Vec<Box<Value>>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for AnyContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> std::result::Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<Box<Value>>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for AnyContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<Box<Value>>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(AnyContainer {
			data: h.data,
		})
	}
}

impl<S: Storage> Deref for AnyContainer<S> {
	type Target = [Box<Value>];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl AnyContainer<Cow> {
	pub fn new(data: Vec<Box<Value>>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<Box<Value>>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<S: Storage> AnyContainer<S> {
	pub fn from_parts(data: S::Vec<Box<Value>>) -> Self {
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

	pub fn push(&mut self, value: Box<Value>) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, Box::new(Value::none()));
	}

	pub fn get(&self, index: usize) -> Option<&Box<Value>> {
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

	pub fn data(&self) -> &S::Vec<Box<Value>> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<Box<Value>> {
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
			Value::Any(self.data[index].clone())
		} else {
			Value::none()
		}
	}

	pub fn none_count(&self) -> usize {
		0
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
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
				DataVec::push(&mut new_data, Box::new(Value::none()));
			}
		}

		self.data = new_data;
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		Ok(())
	}
}
