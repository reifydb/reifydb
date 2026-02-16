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
	value::{Value, identity::IdentityId, r#type::Type},
};

pub struct IdentityIdContainer<S: Storage = Cow> {
	data: S::Vec<IdentityId>,
}

impl<S: Storage> Clone for IdentityIdContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
		}
	}
}

impl<S: Storage> Debug for IdentityIdContainer<S>
where
	S::Vec<IdentityId>: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("IdentityIdContainer").field("data", &self.data).finish()
	}
}

impl<S: Storage> PartialEq for IdentityIdContainer<S>
where
	S::Vec<IdentityId>: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data
	}
}

impl Serialize for IdentityIdContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<IdentityId>,
		}
		Helper {
			data: &self.data,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for IdentityIdContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<IdentityId>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(IdentityIdContainer {
			data: h.data,
		})
	}
}

impl<S: Storage> Deref for IdentityIdContainer<S> {
	type Target = [IdentityId];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl IdentityIdContainer<Cow> {
	pub fn new(data: Vec<IdentityId>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn from_vec(data: Vec<IdentityId>) -> Self {
		Self {
			data: CowVec::new(data),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
		}
	}
}

impl<S: Storage> IdentityIdContainer<S> {
	pub fn from_parts(data: S::Vec<IdentityId>) -> Self {
		Self {
			data,
		}
	}

	pub fn len(&self) -> usize {
		DataVec::len(&self.data)
	}

	pub fn is_empty(&self) -> bool {
		DataVec::is_empty(&self.data)
	}

	pub fn clear(&mut self) {
		DataVec::clear(&mut self.data);
	}

	pub fn push(&mut self, value: impl Into<Option<IdentityId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				DataVec::push(&mut self.data, id);
			}
			None => {
				DataVec::push(&mut self.data, IdentityId::default());
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

	pub fn data(&self) -> &S::Vec<IdentityId> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<IdentityId> {
		&mut self.data
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len()
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_from_slice(&mut self.data, DataVec::as_slice(&other.data));
		Ok(())
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::IdentityId).unwrap_or(Value::none_of(Type::IdentityId))
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < DataVec::len(&self.data) {
				DataVec::push(&mut new_data, self.data[i]);
			}
		}

		self.data = new_data;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());

		for &index in indices {
			if index < DataVec::len(&self.data) {
				DataVec::push(&mut new_data, self.data[index]);
			} else {
				DataVec::push(&mut new_data, IdentityId::default());
			}
		}

		self.data = new_data;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: DataVec::take(&self.data, num),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i]);
		}
		Self {
			data: new_data,
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data)
	}
}

impl From<Vec<IdentityId>> for IdentityIdContainer<Cow> {
	fn from(data: Vec<IdentityId>) -> Self {
		Self::from_vec(data)
	}
}

impl FromIterator<Option<IdentityId>> for IdentityIdContainer<Cow> {
	fn from_iter<T: IntoIterator<Item = Option<IdentityId>>>(iter: T) -> Self {
		let mut container = Self::with_capacity(0);
		for item in iter {
			container.push(item);
		}
		container
	}
}
