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
	value::{Value, identity::IdentityId, value_type::ValueType},
};

pub struct IdentityIdContainer {
	data: SharedVec<IdentityId>,
}

impl Clone for IdentityIdContainer {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl Debug for IdentityIdContainer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("IdentityIdContainer").field("data", &self.data).finish()
	}
}

impl PartialEq for IdentityIdContainer {
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for IdentityIdContainer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a [IdentityId],
		}
		Helper {
			data: self.data.as_slice(),
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for IdentityIdContainer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: Vec<IdentityId>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(IdentityIdContainer {
			data: SharedVec::from_vec(h.data),
		})
	}
}

impl Deref for IdentityIdContainer {
	type Target = [IdentityId];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl IdentityIdContainer {
	pub fn new(data: Vec<IdentityId>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
		}
	}

	pub fn from_vec(data: Vec<IdentityId>) -> Self {
		Self {
			data: SharedVec::from_vec(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: SharedVec::with_capacity(capacity),
		}
	}
}

impl IdentityIdContainer {
	pub fn from_parts(data: Vec<IdentityId>) -> Self {
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

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, value: impl Into<Option<IdentityId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				self.data.push(id);
			}
			None => {
				self.data.push(IdentityId::default());
			}
		}
	}

	pub fn push_default(&mut self) {
		self.push(None);
	}

	pub fn get(&self, index: usize) -> Option<IdentityId> {
		if index < self.len() {
			Some(self.data[index])
		} else {
			None
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<IdentityId>> + '_ {
		self.data.iter().map(|&id| Some(id))
	}

	pub fn data(&self) -> &[IdentityId] {
		self.data.as_slice()
	}

	pub fn data_mut(&mut self) -> &mut Vec<IdentityId> {
		self.data.make_mut()
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend_from_slice(other.data.as_slice());
		Ok(())
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::IdentityId).unwrap_or(Value::none_of(ValueType::IdentityId))
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.data.len() {
				new_data.push(self.data[i]);
			}
		}

		self.data = SharedVec::from_vec(new_data);
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());

		for &index in indices {
			if index < self.data.len() {
				new_data.push(self.data[index]);
			} else {
				new_data.push(IdentityId::default());
			}
		}

		self.data = SharedVec::from_vec(new_data);
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.slice(0, num),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		Self {
			data: self.data.slice(start, end),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn heap_size(&self) -> usize {
		self.capacity() * size_of::<IdentityId>()
	}
}

impl From<Vec<IdentityId>> for IdentityIdContainer {
	fn from(data: Vec<IdentityId>) -> Self {
		Self::from_vec(data)
	}
}

impl FromIterator<Option<IdentityId>> for IdentityIdContainer {
	fn from_iter<T: IntoIterator<Item = Option<IdentityId>>>(iter: T) -> Self {
		let mut container = Self::with_capacity(0);
		for item in iter {
			container.push(item);
		}
		container
	}
}
