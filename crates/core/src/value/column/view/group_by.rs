// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	iter::{Enumerate, FilterMap},
	vec::IntoIter as VecIntoIter,
};

use indexmap::IndexMap;
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_value::{Result, error::Error, value::Value};

use crate::{
	error::CoreError,
	value::column::{ColumnBuffer, columns::Columns},
};

pub type GroupKey = Vec<Value>;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u32);

impl GroupId {
	pub fn index(self) -> usize {
		self.0 as usize
	}
}

pub type GroupRows = Vec<(GroupId, Vec<usize>)>;

#[derive(Debug, Clone)]
pub struct GroupSlots<T> {
	slots: Vec<Option<T>>,
	occupied: usize,
}

impl<T> Default for GroupSlots<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> GroupSlots<T> {
	pub fn new() -> Self {
		Self {
			slots: Vec::new(),
			occupied: 0,
		}
	}

	pub fn len(&self) -> usize {
		self.occupied
	}

	pub fn is_empty(&self) -> bool {
		self.occupied == 0
	}

	pub fn get(&self, group: GroupId) -> Option<&T> {
		self.slots.get(group.index()).and_then(Option::as_ref)
	}

	pub fn insert(&mut self, group: GroupId, value: T) {
		self.reserve_for(group);
		if self.slots[group.index()].replace(value).is_none() {
			self.occupied += 1;
		}
	}

	pub fn get_or_insert_with(&mut self, group: GroupId, default: impl FnOnce() -> T) -> &mut T {
		self.reserve_for(group);
		if self.slots[group.index()].is_none() {
			self.occupied += 1;
		}
		self.slots[group.index()].get_or_insert_with(default)
	}

	pub fn or_insert(&mut self, group: GroupId, default: T) -> &mut T {
		self.get_or_insert_with(group, || default)
	}

	pub fn remove(&mut self, group: GroupId) -> Option<T> {
		let removed = self.slots.get_mut(group.index()).and_then(Option::take);
		if removed.is_some() {
			self.occupied -= 1;
		}
		removed
	}

	pub fn iter(&self) -> impl Iterator<Item = (GroupId, &T)> {
		self.slots
			.iter()
			.enumerate()
			.filter_map(|(index, slot)| slot.as_ref().map(|value| (GroupId(index as u32), value)))
	}

	pub fn drain(&mut self) -> impl Iterator<Item = (GroupId, T)> + '_ {
		self.occupied = 0;
		self.slots
			.drain(..)
			.enumerate()
			.filter_map(|(index, slot)| slot.map(|value| (GroupId(index as u32), value)))
	}

	fn reserve_for(&mut self, group: GroupId) {
		if self.slots.len() <= group.index() {
			self.slots.resize_with(group.index() + 1, || None);
		}
	}
}

fn occupied_slot<T>((index, slot): (usize, Option<T>)) -> Option<(GroupId, T)> {
	slot.map(|value| (GroupId(index as u32), value))
}

impl<T> IntoIterator for GroupSlots<T> {
	type Item = (GroupId, T);
	type IntoIter = FilterMap<Enumerate<VecIntoIter<Option<T>>>, fn((usize, Option<T>)) -> Option<(GroupId, T)>>;

	fn into_iter(self) -> Self::IntoIter {
		self.slots
			.into_iter()
			.enumerate()
			.filter_map(occupied_slot as fn((usize, Option<T>)) -> Option<(GroupId, T)>)
	}
}

#[derive(Debug, Default, Clone)]
pub struct GroupKeyDict {
	entries: IndexMap<EncodedKey, GroupKey>,
}

impl GroupKeyDict {
	pub fn new() -> Self {
		Self {
			entries: IndexMap::new(),
		}
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn values(&self, group: GroupId) -> Option<&GroupKey> {
		self.entries.get_index(group.index()).map(|(_, values)| values)
	}

	pub fn iter(&self) -> impl Iterator<Item = (GroupId, &GroupKey)> {
		self.entries.values().enumerate().map(|(index, values)| (GroupId(index as u32), values))
	}

	fn intern(&mut self, encoded: &EncodedKey, materialize: impl FnOnce() -> GroupKey) -> GroupId {
		if let Some(index) = self.entries.get_index_of(encoded) {
			return GroupId(index as u32);
		}
		let (index, _) = self.entries.insert_full(encoded.clone(), materialize());
		GroupId(index as u32)
	}
}

impl Columns {
	pub fn group_by_ids(&self, keys: &[&str], dict: &mut GroupKeyDict) -> Result<GroupRows> {
		let row_count = self.columns.first().map_or(0, |c| c.len());
		let key_columns = self.key_columns(keys)?;

		let mut rows_by_group: IndexMap<GroupId, Vec<usize>> = IndexMap::new();

		for row in 0..row_count {
			let mut serializer = KeySerializer::new();
			for column in &key_columns {
				column.extend_key(row, &mut serializer);
			}
			let encoded = serializer.to_encoded_key();

			let group = dict
				.intern(&encoded, || key_columns.iter().map(|column| column.get_value(row)).collect());
			rows_by_group.entry(group).or_default().push(row);
		}

		Ok(rows_by_group.into_iter().collect())
	}

	fn key_columns(&self, keys: &[&str]) -> Result<Vec<&ColumnBuffer>> {
		let mut key_columns: Vec<&ColumnBuffer> = Vec::with_capacity(keys.len());
		for &key in keys {
			let pos = self.names.iter().position(|n| n.text() == key).ok_or_else(|| {
				Error::from(CoreError::FrameError {
					message: format!("Column '{}' not found", key),
				})
			})?;
			key_columns.push(&self.columns[pos]);
		}
		Ok(key_columns)
	}
}
