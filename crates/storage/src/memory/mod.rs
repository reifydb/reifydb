// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Result;
use crate::engine::{Engine, EngineMut, Key, Value};
use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::RangeBounds;

/// An in-memory key-value storage engine
#[derive(Default)]
pub struct Memory(BTreeMap<Key, Value>);

impl Engine for Memory {
	type ScanIterator<'a> = MemoryScanIterator<'a>;

	fn get(&self, key: &Key) -> Result<Option<Value>> {
		Ok(self.0.get(key).cloned())
	}

	fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIterator<'_> {
		MemoryScanIterator(self.0.range(range))
	}
}

impl EngineMut for Memory {
	fn set(&mut self, key: &Key, value: Value) -> Result<()> {
		self.0.insert(key.to_vec(), value);
		Ok(())
	}

	fn remove(&mut self, key: &Key) -> Result<()> {
		self.0.remove(key);
		Ok(())
	}
}

pub struct MemoryScanIterator<'a>(Range<'a, Key, Value>);

impl Iterator for MemoryScanIterator<'_> {
	type Item = Result<(Key, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
	}
}

impl DoubleEndedIterator for MemoryScanIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.0.next_back().map(|(k, v)| Ok((k.clone(), v.clone())))
	}
}
