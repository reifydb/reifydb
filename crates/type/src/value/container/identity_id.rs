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
	value::{Value, identity::IdentityId},
};

pub struct IdentityIdContainer<S: Storage = Cow> {
	data: S::Vec<IdentityId>,
	bitvec: S::BitVec,
}

impl<S: Storage> Clone for IdentityIdContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
		}
	}
}

impl<S: Storage> Debug for IdentityIdContainer<S>
where
	S::Vec<IdentityId>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("IdentityIdContainer").field("data", &self.data).field("bitvec", &self.bitvec).finish()
	}
}

impl<S: Storage> PartialEq for IdentityIdContainer<S>
where
	S::Vec<IdentityId>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec
	}
}

impl Serialize for IdentityIdContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<IdentityId>,
			bitvec: &'a BitVec,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for IdentityIdContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<IdentityId>,
			bitvec: BitVec,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(IdentityIdContainer {
			data: h.data,
			bitvec: h.bitvec,
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
	pub fn new(data: Vec<IdentityId>, bitvec: BitVec) -> Self {
		assert_eq!(data.len(), bitvec.len());
		Self {
			data: CowVec::new(data),
			bitvec,
		}
	}

	pub fn from_vec(data: Vec<IdentityId>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
		}
	}
}

impl<S: Storage> IdentityIdContainer<S> {
	pub fn from_parts(data: S::Vec<IdentityId>, bitvec: S::BitVec) -> Self {
		Self {
			data,
			bitvec,
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
		DataBitVec::clear(&mut self.bitvec);
	}

	pub fn push(&mut self, value: impl Into<Option<IdentityId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				DataVec::push(&mut self.data, id);
				DataBitVec::push(&mut self.bitvec, true);
			}
			None => {
				DataVec::push(&mut self.data, IdentityId::default());
				DataBitVec::push(&mut self.bitvec, false);
			}
		}
	}

	pub fn push_undefined(&mut self) {
		self.push(None);
	}

	pub fn get(&self, index: usize) -> Option<IdentityId> {
		if index < self.len() && DataBitVec::get(&self.bitvec, index) {
			Some(self.data[index])
		} else {
			None
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<IdentityId>> + '_ {
		self.data.iter().zip(DataBitVec::iter(&self.bitvec)).map(|(id, defined)| {
			if defined {
				Some(*id)
			} else {
				None
			}
		})
	}

	pub fn data(&self) -> &S::Vec<IdentityId> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<IdentityId> {
		&mut self.data
	}

	pub fn defined(&self) -> &S::BitVec {
		&self.bitvec
	}

	pub fn defined_mut(&mut self) -> &mut S::BitVec {
		&mut self.bitvec
	}

	pub fn bitvec(&self) -> &S::BitVec {
		&self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && DataBitVec::get(&self.bitvec, idx)
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		DataVec::extend_from_slice(&mut self.data, DataVec::as_slice(&other.data));
		DataBitVec::extend_from(&mut self.bitvec, &other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, count: usize) {
		for _ in 0..count {
			DataVec::push(&mut self.data, IdentityId::default());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::IdentityId).unwrap_or(Value::None)
	}

	pub fn filter(&mut self, mask: &S::BitVec) {
		let mut new_data = DataVec::spawn(&self.data, DataBitVec::count_ones(mask));
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, DataBitVec::count_ones(mask));

		for (i, keep) in DataBitVec::iter(mask).enumerate() {
			if keep && i < DataVec::len(&self.data) {
				DataVec::push(&mut new_data, self.data[i]);
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
			}
		}

		self.data = new_data;
		self.bitvec = new_bitvec;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = DataVec::spawn(&self.data, indices.len());
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, indices.len());

		for &index in indices {
			if index < DataVec::len(&self.data) {
				DataVec::push(&mut new_data, self.data[index]);
				DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, index));
			} else {
				DataVec::push(&mut new_data, IdentityId::default());
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

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let count = (end - start).min(self.len().saturating_sub(start));
		let mut new_data = DataVec::spawn(&self.data, count);
		let mut new_bitvec = DataBitVec::spawn(&self.bitvec, count);
		for i in start..(start + count) {
			DataVec::push(&mut new_data, self.data[i]);
			DataBitVec::push(&mut new_bitvec, DataBitVec::get(&self.bitvec, i));
		}
		Self {
			data: new_data,
			bitvec: new_bitvec,
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
