// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{Value, identity::IdentityId},
};

/// Container for IdentityId values
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdentityIdContainer {
	data: CowVec<IdentityId>,
	bitvec: BitVec,
}

impl IdentityIdContainer {
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

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn push(&mut self, value: impl Into<Option<IdentityId>>) {
		let value = value.into();
		match value {
			Some(id) => {
				self.data.push(id);
				self.bitvec.push(true);
			}
			None => {
				self.data.push(IdentityId::default());
				self.bitvec.push(false);
			}
		}
	}

	pub fn push_undefined(&mut self) {
		self.push(None);
	}

	pub fn get(&self, index: usize) -> Option<IdentityId> {
		if index < self.len() && self.bitvec.get(index) {
			Some(self.data[index])
		} else {
			None
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<IdentityId>> + '_ {
		self.data.iter().zip(self.bitvec.iter()).map(|(id, defined)| {
			if defined {
				Some(*id)
			} else {
				None
			}
		})
	}

	pub fn data(&self) -> &CowVec<IdentityId> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut CowVec<IdentityId> {
		&mut self.data
	}

	pub fn defined(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn defined_mut(&mut self) -> &mut BitVec {
		&mut self.bitvec
	}

	pub fn bitvec(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && self.bitvec.get(idx)
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.data.extend_from_slice(&other.data);
		self.bitvec.extend(&other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, count: usize) {
		self.data.extend(vec![IdentityId::default(); count]);
		for _ in 0..count {
			self.bitvec.push(false);
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		self.get(index).map(Value::IdentityId).unwrap_or(Value::Undefined)
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::new();
		let mut new_defined = BitVec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.data.len() {
				new_data.push(self.data[i]);
				new_defined.push(self.bitvec.get(i));
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_defined;
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());
		let mut new_defined = BitVec::with_capacity(indices.len());

		for &index in indices {
			if index < self.data.len() {
				new_data.push(self.data[index]);
				new_defined.push(self.bitvec.get(index));
			} else {
				new_data.push(IdentityId::default());
				new_defined.push(false);
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_defined;
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
			bitvec: self.bitvec.take(num),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let new_data: Vec<IdentityId> = self.data.iter().skip(start).take(end - start).cloned().collect();
		let new_bitvec: Vec<bool> = self.bitvec.iter().skip(start).take(end - start).collect();
		Self {
			data: CowVec::new(new_data),
			bitvec: BitVec::from_slice(&new_bitvec),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get(index).map(|id| id.to_string()).unwrap_or_else(|| "NULL".to_string())
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}
}

impl Deref for IdentityIdContainer {
	type Target = [IdentityId];

	fn deref(&self) -> &Self::Target {
		&self.data
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
