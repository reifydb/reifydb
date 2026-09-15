// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use serde::{Deserialize, Serialize};

use crate::{
	Result,
	util::bitvec::BitVec,
	value::{Value, digest::Digest},
};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DigestContainer {
	data: Vec<Option<Box<Digest>>>,
}

impl DigestContainer {
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn capacity(&self) -> usize {
		self.data.capacity()
	}

	pub fn heap_size(&self) -> usize {
		self.capacity() * size_of::<Option<Box<Digest>>>()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn push(&mut self, digest: Box<Digest>) {
		self.data.push(Some(digest));
	}

	pub fn push_default(&mut self) {
		self.data.push(None);
	}

	pub fn get(&self, index: usize) -> Option<&Digest> {
		self.data.get(index).and_then(|slot| slot.as_deref())
	}

	pub fn iter(&self) -> impl Iterator<Item = Option<&Digest>> + '_ {
		self.data.iter().map(|slot| slot.as_deref())
	}

	pub fn is_defined(&self, index: usize) -> bool {
		self.get(index).is_some()
	}

	pub fn as_string(&self, index: usize) -> String {
		self.get_value(index).to_string()
	}

	pub fn get_value(&self, index: usize) -> Value {
		match self.get(index) {
			Some(digest) => Value::Digest(Box::new(digest.clone())),
			None => Value::none(),
		}
	}

	pub fn extend(&mut self, other: &Self) -> Result<()> {
		self.data.extend(other.data.iter().cloned());
		Ok(())
	}

	pub fn take(&self, num: usize) -> Self {
		Self {
			data: self.data[..num.min(self.data.len())].to_vec(),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let end = end.min(self.data.len());
		let start = start.min(end);
		Self {
			data: self.data[start..end].to_vec(),
		}
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let data = mem::take(&mut self.data);
		self.data = data.into_iter().zip(mask.iter()).filter_map(|(slot, keep)| keep.then_some(slot)).collect();
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		self.data = indices.iter().map(|&index| self.data.get(index).cloned().flatten()).collect();
	}
}
