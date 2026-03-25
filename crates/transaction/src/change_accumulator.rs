// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::primitive::PrimitiveId,
		change::{Change, ChangeOrigin, Diff},
	},
};

/// Accumulates per-row flow change diffs and produces batched `Change` objects
/// grouped by `PrimitiveId`.
///
/// During DML operations, each row modification pushes a `(PrimitiveId, Diff)`
/// entry. At commit time, `take_changes()` groups entries by origin and produces
/// one `Change` per primitive — eliminating the need for a separate merge pass.
///
/// Supports savepoint/restore via `len()` / `truncate()`.
#[derive(Debug, Default)]
pub struct ChangeAccumulator {
	entries: Vec<(PrimitiveId, Diff)>,
}

impl ChangeAccumulator {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
		}
	}

	/// Track a single diff for a primitive source.
	pub fn track(&mut self, source: PrimitiveId, diff: Diff) {
		self.entries.push((source, diff));
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

	/// Drain all entries and produce batched `Change` objects grouped by `PrimitiveId`.
	///
	/// Each `PrimitiveId` produces a single `Change` with all its diffs collected
	/// in order. The version is stamped at this point.
	pub fn take_changes(&mut self, version: CommitVersion) -> Vec<Change> {
		let entries = std::mem::take(&mut self.entries);
		let mut grouped: BTreeMap<PrimitiveId, Vec<Diff>> = BTreeMap::new();

		for (id, diff) in entries {
			grouped.entry(id).or_default().push(diff);
		}

		grouped.into_iter()
			.map(|(id, diffs)| Change {
				origin: ChangeOrigin::Primitive(id),
				diffs,
				version,
			})
			.collect()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}
