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

/// Accumulates per-row flow change diffs and produces batched `Change` objects
/// grouped by `ShapeId`.
///
/// During DML operations, each row modification pushes a `(ShapeId, Diff)`
/// entry. At commit time, `take_changes()` groups entries by origin and produces
/// one `Change` per shape - eliminating the need for a separate merge pass.
///
/// Supports savepoint/restore via `len()` / `truncate()`.
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

	/// Track a single diff for a shape shape.
	pub fn track(&mut self, shape: ShapeId, diff: Diff) {
		self.entries.push((shape, diff));
	}

	/// Number of tracked entries (used for savepoint snapshots).
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	/// Truncate to a previously saved length (for savepoint restore).
	pub fn truncate(&mut self, len: usize) {
		self.entries.truncate(len);
	}

	/// Clear all accumulated entries.
	pub fn clear(&mut self) {
		self.entries.clear();
	}

	/// Drain all entries and produce batched `Change` objects grouped by `ShapeId`.
	///
	/// Each `ShapeId` produces a single `Change` with all its diffs collected
	/// in order. The version is stamped at this point.
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

	/// Drain entries from `offset` onwards and produce batched `Change` objects.
	/// Entries before `offset` are preserved.
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

	/// Read entries from a given offset without draining.
	/// Used by testing::*::changed() to read mutations since the baseline.
	pub fn entries_from(&self, offset: usize) -> &[(ShapeId, Diff)] {
		if offset >= self.entries.len() {
			&[]
		} else {
			&self.entries[offset..]
		}
	}
}
