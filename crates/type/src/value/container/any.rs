// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{
	util::{bitvec::BitVec, cowvec::CowVec},
	value::Value,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnyContainer {
	data: CowVec<Box<Value>>,
	bitvec: BitVec,
}

impl AnyContainer {
	pub fn new(data: Vec<Box<Value>>, bitvec: BitVec) -> Self {
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

	pub fn from_vec(data: Vec<Box<Value>>) -> Self {
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

	pub fn capacity(&self) -> usize {
		debug_assert!(self.data.capacity() >= self.bitvec.capacity());
		self.data.capacity().min(self.bitvec.capacity())
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn push(&mut self, value: Box<Value>) {
		self.data.push(value);
		self.bitvec.push(true);
	}

	pub fn push_undefined(&mut self) {
		self.data.push(Box::new(Value::Undefined));
		self.bitvec.push(false);
	}

	pub fn get(&self, index: usize) -> Option<&Box<Value>> {
		if index < self.len() && self.is_defined(index) {
			self.data.get(index)
		} else {
			None
		}
	}

	pub fn bitvec(&self) -> &BitVec {
		&self.bitvec
	}

	pub fn bitvec_mut(&mut self) -> &mut BitVec {
		&mut self.bitvec
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		idx < self.len() && self.bitvec.get(idx)
	}

	pub fn is_fully_defined(&self) -> bool {
		self.bitvec.count_ones() == self.len()
	}

	pub fn data(&self) -> &CowVec<Box<Value>> {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut CowVec<Box<Value>> {
		&mut self.data
	}

	pub fn as_string(&self, index: usize) -> String {
		if index < self.len() && self.is_defined(index) {
			format!("{}", self.data[index])
		} else {
			"undefined".to_string()
		}
	}

	pub fn get_value(&self, index: usize) -> Value {
		if index < self.len() && self.is_defined(index) {
			Value::Any(self.data[index].clone())
		} else {
			Value::Undefined
		}
	}

	pub fn undefined_count(&self) -> usize {
		self.bitvec().count_zeros()
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data.take(num),
			bitvec: self.bitvec.take(num),
		}
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

	pub fn reorder(&mut self, indices: &[usize]) {
		let mut new_data = Vec::with_capacity(indices.len());
		let mut new_bitvec = BitVec::with_capacity(indices.len());

		for &idx in indices {
			if idx < self.len() {
				new_data.push(self.data[idx].clone());
				new_bitvec.push(self.bitvec.get(idx));
			} else {
				new_data.push(Box::new(Value::Undefined));
				new_bitvec.push(false);
			}
		}

		self.data = CowVec::new(new_data);
		self.bitvec = new_bitvec;
	}

	pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
		self.data.extend(other.data.iter().cloned());
		self.bitvec.extend(&other.bitvec);
		Ok(())
	}

	pub fn extend_from_undefined(&mut self, len: usize) {
		self.data.extend(std::iter::repeat(Box::new(Value::Undefined)).take(len));
		self.bitvec.extend(&BitVec::repeat(len, false));
	}
}

impl Deref for AnyContainer {
	type Target = [Box<Value>];

	fn deref(&self) -> &Self::Target {
		&self.data
	}
}
