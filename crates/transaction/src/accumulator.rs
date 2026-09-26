// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem,
};

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
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
	cursor: usize,
}

impl ChangeAccumulator {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			cursor: 0,
		}
	}

	pub fn track(&mut self, object: ObjectId, diff: Diff) {
		self.entries.push((object, diff));
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn cursor(&self) -> usize {
		self.cursor
	}

	pub fn set_cursor(&mut self, at: usize) {
		assert!(
			at <= self.entries.len(),
			"accumulator cursor {} past accumulator length {}",
			at,
			self.entries.len()
		);
		self.cursor = at;
	}

	pub fn truncate(&mut self, len: usize) {
		self.entries.truncate(len);
		self.cursor = self.cursor.min(self.entries.len());
	}

	pub fn clear(&mut self) {
		self.entries.clear();
		self.cursor = 0;
	}

	pub fn take_changes(&mut self, version: CommitVersion, changed_at: DateTime) -> Result<Vec<Change>> {
		let entries = mem::take(&mut self.entries);
		self.cursor = 0;
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
		self.cursor = self.cursor.min(self.entries.len());
		build_changes(tail, version, changed_at)
	}

	pub fn entries_from(&self, offset: usize) -> &[(ObjectId, Diff)] {
		if offset >= self.entries.len() {
			&[]
		} else {
			&self.entries[offset..]
		}
	}

	pub fn pending_objects(&self) -> Vec<ObjectId> {
		let mut seen = BTreeSet::new();
		self.entries.iter().map(|(object, _)| *object).filter(|object| seen.insert(*object)).collect()
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
			version: ChangeVersion::from(version),
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

	#[test]
	fn test_new_cursor_is_zero_so_first_pass_sees_every_entry() {
		let mut accumulator = ChangeAccumulator::new();
		assert_eq!(accumulator.cursor(), 0);

		let table = ObjectId::Table(TableId(1));
		accumulator.track(table, Diff::insert(Columns::empty()));
		let seen: Vec<ObjectId> =
			accumulator.entries_from(accumulator.cursor()).iter().map(|(object, _)| *object).collect();
		assert_eq!(seen, vec![table]);
	}

	#[test]
	fn test_entries_from_cursor_skips_entries_already_processed() {
		let mut accumulator = ChangeAccumulator::new();
		let first = ObjectId::Table(TableId(1));
		let second = ObjectId::Table(TableId(2));
		let third = ObjectId::View(ViewId(3));

		accumulator.track(first, Diff::insert(Columns::empty()));
		accumulator.track(second, Diff::insert(Columns::empty()));
		accumulator.set_cursor(2);
		accumulator.track(third, Diff::insert(Columns::empty()));

		assert_eq!(accumulator.cursor(), 2);
		let seen: Vec<ObjectId> =
			accumulator.entries_from(accumulator.cursor()).iter().map(|(object, _)| *object).collect();
		assert_eq!(
			seen,
			vec![third],
			"entries before the cursor were already processed and must not be seen again"
		);
	}

	#[test]
	fn test_truncate_below_cursor_clamps_it_so_new_entries_are_not_skipped() {
		let mut accumulator = ChangeAccumulator::new();
		for id in 1..=3 {
			accumulator.track(ObjectId::Table(TableId(id)), Diff::insert(Columns::empty()));
		}
		accumulator.set_cursor(3);

		accumulator.truncate(1);
		assert_eq!(accumulator.cursor(), 1, "a cursor past the end would skip entries tracked after rollback");

		let replacement = ObjectId::View(ViewId(9));
		accumulator.track(replacement, Diff::insert(Columns::empty()));
		let seen: Vec<ObjectId> =
			accumulator.entries_from(accumulator.cursor()).iter().map(|(object, _)| *object).collect();
		assert_eq!(seen, vec![replacement]);
	}

	#[test]
	fn test_truncate_above_cursor_keeps_it_so_processed_entries_are_not_reprocessed() {
		let mut accumulator = ChangeAccumulator::new();
		for id in 1..=3 {
			accumulator.track(ObjectId::Table(TableId(id)), Diff::insert(Columns::empty()));
		}
		accumulator.set_cursor(1);

		accumulator.truncate(2);
		assert_eq!(accumulator.cursor(), 1);
	}

	#[test]
	fn test_take_changes_resets_cursor_so_later_entries_are_not_skipped() {
		let mut accumulator = ChangeAccumulator::new();
		accumulator.track(ObjectId::Table(TableId(1)), Diff::insert(Columns::empty()));
		accumulator.track(ObjectId::Table(TableId(2)), Diff::insert(Columns::empty()));
		accumulator.set_cursor(2);

		accumulator.take_changes(CommitVersion(1), DateTime::default()).unwrap();
		assert_eq!(accumulator.cursor(), 0);

		let next = ObjectId::View(ViewId(3));
		accumulator.track(next, Diff::insert(Columns::empty()));
		let seen: Vec<ObjectId> =
			accumulator.entries_from(accumulator.cursor()).iter().map(|(object, _)| *object).collect();
		assert_eq!(seen, vec![next]);
	}

	#[test]
	fn test_take_changes_from_below_cursor_clamps_cursor_to_offset() {
		let mut accumulator = ChangeAccumulator::new();
		for id in 1..=3 {
			accumulator.track(ObjectId::Table(TableId(id)), Diff::insert(Columns::empty()));
		}
		accumulator.set_cursor(3);

		accumulator.take_changes_from(1, CommitVersion(1), DateTime::default()).unwrap();
		assert_eq!(accumulator.len(), 1);
		assert_eq!(accumulator.cursor(), 1, "the cursor must never point past the remaining entries");
	}

	#[test]
	fn test_clear_resets_cursor_so_later_entries_are_not_skipped() {
		let mut accumulator = ChangeAccumulator::new();
		accumulator.track(ObjectId::Table(TableId(1)), Diff::insert(Columns::empty()));
		accumulator.set_cursor(1);

		accumulator.clear();
		assert_eq!(accumulator.cursor(), 0);
	}

	#[test]
	#[should_panic(expected = "accumulator cursor 2 past accumulator length 1")]
	fn test_set_cursor_past_length_panics_because_it_would_skip_future_entries() {
		let mut accumulator = ChangeAccumulator::new();
		accumulator.track(ObjectId::Table(TableId(1)), Diff::insert(Columns::empty()));
		accumulator.set_cursor(2);
	}
}
