// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use reifydb_type::{Value, VarInt};
use serde::{Deserialize, Serialize};

use crate::{BitVec, CowVec};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VarIntContainer {
	data: CowVec<VarInt>,
	bitvec: BitVec,
}

impl Deref for VarIntContainer {
	type Target = [VarInt];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

impl VarIntContainer {
	pub fn new(data: Vec<VarInt>, bitvec: BitVec) -> Self {
		debug_assert_eq!(data.len(), bitvec.len());
		Self {
			data: CowVec::new(data),
			bitvec,
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: CowVec::with_capacity(capacity),
			bitvec: BitVec::with_capacity(capacity),
		}
	}

	pub fn from_vec(data: Vec<VarInt>) -> Self {
		let len = data.len();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::repeat(len, true),
		}
	}

	pub fn len(&self) -> usize {
		debug_assert_eq!(self.data.len(), self.bitvec.len());
		self.data.len()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn bitvec(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn as_slice(&self) -> &[VarInt] {
		self.data.as_slice()
	}

	pub fn is_defined(&self, index: usize) -> bool {
		index < self.len() && self.bitvec.get(index)
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn push_undefined(&mut self) {
		self.data.push(VarInt::from(0));
		self.bitvec.push(false);
	}

	pub fn data(&self) -> &[VarInt] {
		self.data.as_slice()
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			self.data[index].to_string()
		} else {
			String::new()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if self.is_defined(index) {
			Value::VarInt(self.data[index].clone())
		} else {
			Value::Undefined
		}
	}

	pub fn push(&mut self, value: Value) {
		match value {
			Value::VarInt(v) => {
				self.data.push(v);
				self.bitvec.push(true);
			}
			Value::Undefined => {
				self.data.push(VarInt::from(0));
				self.bitvec.push(false);
			}
			_ => unreachable!(
				"VarIntContainer::push with invalid value: {value:?}"
			),
		}
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.data.extend(other.data.iter().cloned());
		self.bitvec.extend(&other.bitvec);
		Ok(())
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		debug_assert_eq!(indices.len(), self.len());

		let mut new_data = Vec::with_capacity(self.len());
		let mut new_bitvec = BitVec::with_capacity(self.len());

		for &idx in indices {
			new_data.push(self.data[idx].clone());
			new_bitvec.push(self.bitvec.get(idx));
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn take(&self, num: usize) -> Self {
		let count = num.min(self.len());
		let data: Vec<VarInt> =
			self.data.iter().take(count).cloned().collect();
		let bitvec_data: Vec<bool> =
			self.bitvec.iter().take(count).collect();
		Self {
			data: CowVec::new(data),
			bitvec: BitVec::from_slice(&bitvec_data),
		}
	}

	pub fn take_indices(&mut self, indices: &[usize]) -> Self {
		let mut data = Vec::with_capacity(indices.len());
		let mut bitvec = BitVec::with_capacity(indices.len());

		for &idx in indices {
			data.push(self.data[idx].clone());
			bitvec.push(self.bitvec.get(idx));
		}

		Self::new(data, bitvec)
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let mut new_data = Vec::with_capacity(mask.count_ones());
		let mut new_bitvec = BitVec::with_capacity(mask.count_ones());

		for (i, keep) in mask.iter().enumerate() {
			if keep && i < self.len() {
				new_data.push(self.data[i].clone());
				new_bitvec.push(self.bitvec.get(i));
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let new_data: Vec<VarInt> = self
			.data
			.iter()
			.skip(start)
			.take(end - start)
			.cloned()
			.collect();
		let new_bitvec: Vec<bool> = self
			.bitvec
			.iter()
			.skip(start)
			.take(end - start)
			.collect();
		Self {
			data: CowVec::new(new_data),
			bitvec: BitVec::from_slice(&new_bitvec),
		}
	}

	pub fn extend_from_undefined(&mut self, count: usize) {
		for _ in 0..count {
			self.data.push(VarInt::from(0));
			self.bitvec.push(false);
		}
	}
}
