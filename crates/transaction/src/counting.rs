// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::key::tag::KeyTag;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyCounts {
	pub gets: u64,
	pub committed_gets: u64,
	pub contains: u64,
	pub sets: u64,
	pub removes: u64,
	pub get_calls: u64,
	pub committed_get_calls: u64,
	pub contains_calls: u64,
	pub set_calls: u64,
	pub remove_calls: u64,
	last_call: u64,
}

#[derive(Clone, Debug, Default)]
pub struct OpCounts {
	keys: HashMap<KeyTag, KeyCounts>,
	call: u64,
	set_calls: u64,
	get_calls: u64,
	get_committed_calls: u64,
	contains_calls: u64,
	remove_calls: u64,
	ranges: u64,
}

impl OpCounts {
	pub fn of(&self, tag: KeyTag) -> KeyCounts {
		self.keys.get(&tag).copied().unwrap_or_default()
	}

	pub fn set_calls(&self) -> u64 {
		self.set_calls
	}

	pub fn get_calls(&self) -> u64 {
		self.get_calls
	}

	pub fn get_committed_calls(&self) -> u64 {
		self.get_committed_calls
	}

	pub fn contains_calls(&self) -> u64 {
		self.contains_calls
	}

	pub fn remove_calls(&self) -> u64 {
		self.remove_calls
	}

	pub fn ranges(&self) -> u64 {
		self.ranges
	}

	pub(crate) fn key(&mut self, tag: KeyTag) -> &mut KeyCounts {
		self.keys.entry(tag).or_default()
	}

	pub(crate) fn set_call(&mut self, kinds: impl IntoIterator<Item = KeyTag>) {
		self.set_calls += 1;
		self.per_kind(kinds, |counts| &mut counts.sets, |counts| &mut counts.set_calls);
	}

	pub(crate) fn get_call(&mut self, kinds: impl IntoIterator<Item = KeyTag>) {
		self.get_calls += 1;
		self.per_kind(kinds, |counts| &mut counts.gets, |counts| &mut counts.get_calls);
	}

	pub(crate) fn get_committed_call(&mut self, kinds: impl IntoIterator<Item = KeyTag>) {
		self.get_committed_calls += 1;
		self.per_kind(kinds, |counts| &mut counts.committed_gets, |counts| &mut counts.committed_get_calls);
	}

	pub(crate) fn contains_call(&mut self, kinds: impl IntoIterator<Item = KeyTag>) {
		self.contains_calls += 1;
		self.per_kind(kinds, |counts| &mut counts.contains, |counts| &mut counts.contains_calls);
	}

	pub(crate) fn remove_call(&mut self, kinds: impl IntoIterator<Item = KeyTag>) {
		self.remove_calls += 1;
		self.per_kind(kinds, |counts| &mut counts.removes, |counts| &mut counts.remove_calls);
	}

	fn per_kind(
		&mut self,
		kinds: impl IntoIterator<Item = KeyTag>,
		per_key: fn(&mut KeyCounts) -> &mut u64,
		per_call: fn(&mut KeyCounts) -> &mut u64,
	) {
		self.call += 1;
		let call = self.call;
		for kind in kinds {
			let counts = self.key(kind);
			*per_key(counts) += 1;
			if counts.last_call != call {
				counts.last_call = call;
				*per_call(counts) += 1;
			}
		}
	}

	pub(crate) fn range_call(&mut self) {
		self.ranges += 1;
	}
}
