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
	util::bitvec::BitVec,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	},
};

pub struct DictionaryContainer {
	data: Vec<DictionaryEntryId>,
	dictionary_id: Option<DictionaryId>,
}

impl Clone for DictionaryContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			dictionary_id: self.dictionary_id,
		}
	}
}

impl Debug for DictionaryContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DictionaryContainer")
			.field("data", &self.data)
			.field("dictionary_id", &self.dictionary_id)
			.finish()
	}
}

impl PartialEq for DictionaryContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.dictionary_id == other.dictionary_id
	}
}

impl Serialize for DictionaryContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a Vec<DictionaryEntryId>,
			dictionary_id: Option<DictionaryId>,
		}
		Helper {
			data: &self.data,
			dictionary_id: self.dictionary_id,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for DictionaryContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: Vec<DictionaryEntryId>,
			dictionary_id: Option<DictionaryId>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(DictionaryContainer {
			data: h.data,
			dictionary_id: h.dictionary_id,
		})
	}
}

impl Deref for DictionaryContainer {
	type Target = [DictionaryEntryId];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl DictionaryContainer {
	pub fn new(data: Vec<DictionaryEntryId>) -> Self {
		Self {
			data,
			dictionary_id: None,
		}
	}

	pub fn from_vec(data: Vec<DictionaryEntryId>) -> Self {
		Self {
			data,
			dictionary_id: None,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
			dictionary_id: None,
		}
	}
}

impl DictionaryContainer {
	pub fn from_parts(data: Vec<DictionaryEntryId>, dictionary_id: Option<DictionaryId>) -> Self {
		Self {
			data,
			dictionary_id,
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: impl Into<Option<DictionaryEntryId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				self.data.push(id);
			}
			None => {
				self.data.push(DictionaryEntryId::default());
			}
		}
	}

	pub fn push_default(&mut self) {
		self.push(None);
	}

	pub fn get(&self, index: usize) -> Option<DictionaryEntryId> {
		if index < self.len() {
			Some(self.data[index])
		} else {
			None
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<DictionaryEntryId>> + '_ {
		self.data.iter().map(|&id| Some(id))
	}

	pub fn data(&self) -> &Vec<DictionaryEntryId> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut Vec<DictionaryEntryId> {
		&mut self.data
	}

	pub fn dictionary_id(&self) -> Option<DictionaryId> {
		self.dictionary_id
	}

	pub fn set_dictionary_id(&mut self, id: DictionaryId) {
		self.dictionary_id = Some(id);
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend_from_slice(other.data.as_slice());
		Ok(())
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::DictionaryId).unwrap_or(Value::none_of(ValueType::DictionaryId))
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.data.len() {
				new_data.push(self.data[i]);
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());

		for &index in indices {
			if index < self.data.len() {
				new_data.push(self.data[index]);
			} else {
				new_data.push(DictionaryEntryId::default());
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
			dictionary_id: self.dictionary_id,
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = Vec::with_capacity(count);
		for i in start..(start + count) {
			new_data.push(self.data[i]);
		}
		Self {
			data: new_data,
			dictionary_id: self.dictionary_id,
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn heap_size(&self) -> usize {
		self.capacity() * size_of::<DictionaryEntryId>()
	}
}

impl From<Vec<DictionaryEntryId>> for DictionaryContainer {
	fn from(data: Vec<DictionaryEntryId>) -> Self {
		Self::from_vec(data)
	}
}

impl FromIterator<Option<DictionaryEntryId>> for DictionaryContainer {
	fn from_iter<T: IntoIterator<Item = Option<DictionaryEntryId>>>(iter: T) -> Self {
		let mut container = Self::with_capacity(0);
		for item in iter {
			container.push(item);
		}
		container
	}
}
