// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, mem};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::shape::ShapeId,
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_type::value::datetime::DateTime;

#[derive(Debug, Default)]
pub struct ChangeAccumulator {
	entries: Vec<(ShapeId, Diff)>,
}

impl ChangeAccumulator {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
		}
	}

	pub fn track(&mut self, shape: ShapeId, diff: Diff) {
		self.entries.push((shape, diff));
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn truncate(&mut self, len: usize) {
		self.entries.truncate(len);
	}

	pub fn clear(&mut self) {
		self.entries.clear();
	}

	pub fn take_changes(&mut self, version: CommitVersion, changed_at: DateTime) -> Vec<Change> {
		let entries = mem::take(&mut self.entries);
		let mut grouped: BTreeMap<ShapeId, Vec<Diff>> = BTreeMap::new();

		for (id, diff) in entries {
			grouped.entry(id).or_default().push(diff);
		}

		grouped.into_iter()
			.map(|(id, diffs)| Change {
				origin: ChangeOrigin::Shape(id),
				diffs: diffs.into(),
				version,
				changed_at,
			})
			.collect()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn take_changes_from(
		&mut self,
		offset: usize,
		version: CommitVersion,
		changed_at: DateTime,
	) -> Vec<Change> {
		if offset >= self.entries.len() {
			return Vec::new();
		}
		let tail = self.entries.split_off(offset);
		let mut grouped: BTreeMap<ShapeId, Vec<Diff>> = BTreeMap::new();
		for (id, diff) in tail {
			grouped.entry(id).or_default().push(diff);
		}
		grouped.into_iter()
			.map(|(id, diffs)| Change {
				origin: ChangeOrigin::Shape(id),
				diffs: diffs.into(),
				version,
				changed_at,
			})
			.collect()
	}

	pub fn entries_from(&self, offset: usize) -> &[(ShapeId, Diff)] {
		if offset >= self.entries.len() {
			&[]
		} else {
			&self.entries[offset..]
		}
	}
}
