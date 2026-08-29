// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, btree_map},
	ops::Bound,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::tier::resident::batch::{StateEntry, StateKey};

pub type OperatorKeys = BTreeMap<EncodedKey, StateEntry>;

#[derive(Debug, Default)]
pub struct StateMap {
	operators: BTreeMap<OperatorId, OperatorKeys>,
	len: usize,
}

impl StateMap {
	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn get(&self, key: &StateKey) -> Option<&StateEntry> {
		self.lookup(key.0, &key.1)
	}

	pub fn contains_key(&self, key: &StateKey) -> bool {
		self.get(key).is_some()
	}

	pub fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<&StateEntry> {
		self.operators.get(&operator)?.get(key)
	}

	pub fn iter(&self) -> Iter<'_> {
		Iter {
			outer: self.operators.iter(),
			current: None,
		}
	}

	pub fn range(&self, operator: OperatorId, lower: Bound<EncodedKey>, upper: Bound<EncodedKey>) -> Range<'_> {
		Range {
			inner: self.operators.get(&operator).map(|keys| keys.range((lower, upper))),
		}
	}

	pub(super) fn insert(&mut self, operator: OperatorId, key: EncodedKey, entry: StateEntry) {
		if self.operators.entry(operator).or_default().insert(key, entry).is_none() {
			self.len += 1;
		}
	}
}

#[derive(Default)]
pub struct Range<'a> {
	inner: Option<btree_map::Range<'a, EncodedKey, StateEntry>>,
}

impl<'a> Iterator for Range<'a> {
	type Item = (&'a EncodedKey, &'a StateEntry);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.as_mut()?.next()
	}
}

impl<'a> DoubleEndedIterator for Range<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.inner.as_mut()?.next_back()
	}
}

pub struct Iter<'a> {
	outer: btree_map::Iter<'a, OperatorId, OperatorKeys>,
	current: Option<(OperatorId, btree_map::Iter<'a, EncodedKey, StateEntry>)>,
}

impl<'a> Iterator for Iter<'a> {
	type Item = ((OperatorId, &'a EncodedKey), &'a StateEntry);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some((operator, keys)) = self.current.as_mut()
				&& let Some((key, entry)) = keys.next()
			{
				return Some(((*operator, key), entry));
			}
			let (operator, keys) = self.outer.next()?;
			self.current = Some((*operator, keys.iter()));
		}
	}
}

impl<'a> IntoIterator for &'a StateMap {
	type Item = ((OperatorId, &'a EncodedKey), &'a StateEntry);
	type IntoIter = Iter<'a>;

	fn into_iter(self) -> Iter<'a> {
		self.iter()
	}
}
