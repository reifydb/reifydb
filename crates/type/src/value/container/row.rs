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
	value::{Value, row_number::RowNumber, r#type::Type},
};

pub struct RowNumberContainer<S: Storage = Cow> {
	data: S::Vec<RowNumber>,
}

impl<S: Storage> Clone for RowNumberContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for RowNumberContainer<S>
where
	S::Vec<RowNumber>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RowNumberContainer").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for RowNumberContainer<S>
where
	S::Vec<RowNumber>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for RowNumberContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> std::result::Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<RowNumber>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for RowNumberContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<RowNumber>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(RowNumberContainer {
			data: h.data,
		})
	}
}

impl<S: Storage> Deref for RowNumberContainer<S> {
	type Target = [RowNumber];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl RowNumberContainer<Cow> {
	pub fn new(data: Vec<RowNumber>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<RowNumber>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}
}

impl<S: Storage> RowNumberContainer<S> {
	pub fn from_parts(data: S::Vec<RowNumber>) -> Self {
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

	pub fn push(&mut self, value: RowNumber) {
		DataVec::push(&mut self.data, value);
	}

	pub fn push_default(&mut self) {
		DataVec::push(&mut self.data, RowNumber::default());
	}

	pub fn get(&self, index: usize) -> Option<&RowNumber> {
		if index < self.len() {
			DataVec::get(&self.data, index)
		} else {
			None
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn data(&self) -> &S::Vec<RowNumber> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<RowNumber> {
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
			Value::Uint8(self.data[index].value())
		} else {
			Value::none_of(Type::Uint8)
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		DataVec::extend_iter(&mut self.data, other.data.iter().cloned());
		Ok(())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<RowNumber>> + '_ {
		self.data.iter().map(|&v| Some(v))
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
				DataVec::push(&mut new_data, RowNumber::default());
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

impl Default for RowNumberContainer<Cow> {
	fn default() -> Self {
		Self::with_capacity(0)
	}
}
