// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem,
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::object::ObjectId,
		change::{Change, ChangeOrigin, Diff},
		consolidate::consolidate_diffs,
	},
};
use reifydb_value::{Result, value::datetime::DateTime};

#[derive(Debug, Default)]
pub struct ChangeAccumulator {
	entries: Vec<(ObjectId, Diff)>,
}

impl ChangeAccumulator {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
		}
	}

	pub fn track(&mut self, object: ObjectId, diff: Diff) {
		self.entries.push((object, diff));
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

	pub fn take_changes(&mut self, version: CommitVersion, changed_at: DateTime) -> Result<Vec<Change>> {
		let entries = mem::take(&mut self.entries);
		build_changes(entries, version, changed_at)
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn take_changes_from(
		&mut self,
		offset: usize,
		version: CommitVersion,
		changed_at: DateTime,
	) -> Result<Vec<Change>> {
		if offset >= self.entries.len() {
			return Ok(Vec::new());
		}
		let tail = self.entries.split_off(offset);
		build_changes(tail, version, changed_at)
	}

	pub fn take_changes_matching(
		&mut self,
		offset: usize,
		objects: &BTreeSet<ObjectId>,
		version: CommitVersion,
		changed_at: DateTime,
	) -> Result<Vec<Change>> {
		if offset >= self.entries.len() {
			return Ok(Vec::new());
		}
		let tail = self.entries.split_off(offset);
		let (matched, retained): (Vec<_>, Vec<_>) =
			tail.into_iter().partition(|(object, _)| objects.contains(object));
		self.entries.extend(retained);
		build_changes(matched, version, changed_at)
	}

	pub fn entries_from(&self, offset: usize) -> &[(ObjectId, Diff)] {
		if offset >= self.entries.len() {
			&[]
		} else {
			&self.entries[offset..]
		}
	}

	pub fn pending_objects(&self) -> Vec<ObjectId> {
		self.pending_objects_from(0)
	}

	pub fn pending_objects_from(&self, offset: usize) -> Vec<ObjectId> {
		let mut seen = BTreeSet::new();
		self.entries_from(offset)
			.iter()
			.map(|(object, _)| *object)
			.filter(|object| seen.insert(*object))
			.collect()
	}
}

fn build_changes(entries: Vec<(ObjectId, Diff)>, version: CommitVersion, changed_at: DateTime) -> Result<Vec<Change>> {
	let mut grouped: BTreeMap<ObjectId, Vec<Diff>> = BTreeMap::new();
	for (id, diff) in entries {
		grouped.entry(id).or_default().push(diff);
	}

	let mut result: Vec<Change> = Vec::with_capacity(grouped.len());
	for (id, diffs) in grouped {
		let coalesced = consolidate_diffs(diffs)?;
		if coalesced.is_empty() {
			continue;
		}
		result.push(Change {
			origin: ChangeOrigin::Object(id),
			diffs: coalesced.into(),
			version,
			changed_at,
		});
	}
	Ok(result)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::id::{TableId, ViewId},
		value::column::columns::Columns,
	};

	use super::*;

	#[test]
	fn test_pending_objects_dedupes_and_keeps_first_seen_order() {
		let mut accumulator = ChangeAccumulator::new();
		let table = ObjectId::Table(TableId(1));
		let view = ObjectId::View(ViewId(2));

		accumulator.track(table, Diff::insert(Columns::empty()));
		accumulator.track(view, Diff::insert(Columns::empty()));
		accumulator.track(table, Diff::insert(Columns::empty()));

		assert_eq!(
			accumulator.pending_objects(),
			vec![table, view],
			"repeat writes to an object must not duplicate it"
		);
	}

	#[test]
	fn test_pending_objects_empty() {
		assert!(ChangeAccumulator::new().pending_objects().is_empty());
	}
}
