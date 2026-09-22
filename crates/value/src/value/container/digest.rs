// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	util::shared_vec::SharedVec,
	value::{Value, digest::Digest},
};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DigestContainer {
	data: SharedVec<Option<Box<Digest>>>,
}

impl DigestContainer {
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			data: SharedVec::with_capacity(capacity),
		}
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn freeze(&mut self) {
		self.data.freeze();
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
		self.data.make_mut().extend(other.data.iter().cloned());
		Ok(())
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

	pub fn filter(&mut self, mask: &BooleanBuffer) {
		let data = mem::take(self.data.make_mut());
		self.data = data.into_iter().zip(mask.iter()).filter_map(|(slot, keep)| keep.then_some(slot)).collect();
	}

	pub fn reorder(&mut self, indices: &[usize]) {
		self.data = indices.iter().map(|&index| self.data.get(index).cloned().flatten()).collect();
	}
}
