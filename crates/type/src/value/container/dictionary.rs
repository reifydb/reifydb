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
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub struct DictionaryContainer<S: Storage = Cow> {
	data: S::Vec<DictionaryEntryId>,
	bitvec: S::BitVec,
	dictionary_id: Option<DictionaryId>,
}

impl<S: Storage> Clone for DictionaryContainer<S> {
	fn clone(&self) -> Self {
		Self {
			data: self.data.clone(),
			bitvec: self.bitvec.clone(),
			dictionary_id: self.dictionary_id,
		}
	}
}

impl<S: Storage> Debug for DictionaryContainer<S>
where
	S::Vec<DictionaryEntryId>: Debug,
	S::BitVec: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DictionaryContainer")
			.field("data", &self.data)
			.field("bitvec", &self.bitvec)
			.field("dictionary_id", &self.dictionary_id)
			.finish()
	}
}

impl<S: Storage> PartialEq for DictionaryContainer<S>
where
	S::Vec<DictionaryEntryId>: PartialEq,
	S::BitVec: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		self.data == other.data && self.bitvec == other.bitvec && self.dictionary_id == other.dictionary_id
	}
}

impl Serialize for DictionaryContainer<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		struct Helper<'a> {
			data: &'a CowVec<DictionaryEntryId>,
			bitvec: &'a BitVec,
			dictionary_id: Option<DictionaryId>,
		}
		Helper {
			data: &self.data,
			bitvec: &self.bitvec,
			dictionary_id: self.dictionary_id,
		}
		.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for DictionaryContainer<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		struct Helper {
			data: CowVec<DictionaryEntryId>,
			bitvec: BitVec,
			dictionary_id: Option<DictionaryId>,
		}
		let h = Helper::deserialize(deserializer)?;
		Ok(DictionaryContainer {
			data: h.data,
			bitvec: h.bitvec,
			dictionary_id: h.dictionary_id,
		})
	}
}

impl<S: Storage> Deref for DictionaryContainer<S> {
	type Target = [DictionaryEntryId];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl DictionaryContainer<Cow> {
	pub fn new(data: Vec<DictionaryEntryId>, bitvec: BitVec) -> Self {
		assert_eq!(data.len(), bitvec.len());
		Self {
			data: CowVec::new(data),
			bitvec,
			dictionary_id: None,
		}
	}

	pub fn from_vec(data: Vec<DictionaryEntryId>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
			dictionary_id: None,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
			dictionary_id: None,
		}
	}
}

impl<S: Storage> DictionaryContainer<S> {
	pub fn from_parts(
		data: S::Vec<DictionaryEntryId>,
		bitvec: S::BitVec,
		dictionary_id: Option<DictionaryId>,
	) -> Self {
		Self {
			data,
			bitvec,
			dictionary_id,
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

	pub fn push(&mut self, value: impl Into<Option<DictionaryEntryId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				DataVec::push(&mut self.data, id);
				DataBitVec::push(&mut self.bitvec, true);
			}
			None => {
				DataVec::push(&mut self.data, DictionaryEntryId::default());
				DataBitVec::push(&mut self.bitvec, false);
			}
		}
	}

	pub fn push_undefined(&mut self) {
		self.push(None);
	}

	pub fn get(&self, index: usize) -> Option<DictionaryEntryId> {
		if index < self.len() && DataBitVec::get(&self.bitvec, index) {
			Some(self.data[index])
		} else {
			None
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<DictionaryEntryId>> + '_ {
		self.data.iter().zip(DataBitVec::iter(&self.bitvec)).map(|(id, defined)| {
			if defined {
				Some(*id)
			} else {
				None
			}
		})
	}

	pub fn data(&self) -> &S::Vec<DictionaryEntryId> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut S::Vec<DictionaryEntryId> {
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

	pub fn dictionary_id(&self) -> Option<DictionaryId> {
		self.dictionary_id
	}

	pub fn set_dictionary_id(&mut self, id: DictionaryId) {
		self.dictionary_id = Some(id);
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
			DataVec::push(&mut self.data, DictionaryEntryId::default());
			DataBitVec::push(&mut self.bitvec, false);
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::DictionaryId).unwrap_or(Value::None)
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
				DataVec::push(&mut new_data, DictionaryEntryId::default());
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
			dictionary_id: self.dictionary_id,
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
			dictionary_id: self.dictionary_id,
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
	}

	pub fn capacity(&self) -> usize {
		DataVec::capacity(&self.data)
	}
}

impl From<Vec<DictionaryEntryId>> for DictionaryContainer<Cow> {
	fn from(data: Vec<DictionaryEntryId>) -> Self {
		Self::from_vec(data)
	}
}

impl FromIterator<Option<DictionaryEntryId>> for DictionaryContainer<Cow> {
	fn from_iter<T: IntoIterator<Item = Option<DictionaryEntryId>>>(iter: T) -> Self {
		let mut container = Self::with_capacity(0);
		for item in iter {
			container.push(item);
		}
		container
	}
}
