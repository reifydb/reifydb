// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::key::tag::KeyTag;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyCounts {
	pub gets: u64,
	pub contains: u64,
	pub sets: u64,
	pub batched: u64,
}

#[derive(Clone, Debug, Default)]
pub struct OpCounts {
	keys: HashMap<KeyTag, KeyCounts>,
	set_many_calls: u64,
}

impl OpCounts {
	pub fn of(&self, tag: KeyTag) -> KeyCounts {
		self.keys.get(&tag).copied().unwrap_or_default()
	}

	pub fn set_many_calls(&self) -> u64 {
		self.set_many_calls
	}

	pub(crate) fn key(&mut self, tag: KeyTag) -> &mut KeyCounts {
		self.keys.entry(tag).or_default()
	}

	pub(crate) fn set_many_call(&mut self) {
		self.set_many_calls += 1;
	}
}
