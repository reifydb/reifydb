// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, mem, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::shape::ShapeId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::columns::Columns,
};
use reifydb_type::{Result, value::datetime::DateTime};

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

	pub fn entries_from(&self, offset: usize) -> &[(ShapeId, Diff)] {
		if offset >= self.entries.len() {
			&[]
		} else {
			&self.entries[offset..]
		}
	}
}

fn build_changes(entries: Vec<(ShapeId, Diff)>, version: CommitVersion, changed_at: DateTime) -> Result<Vec<Change>> {
	let mut grouped: BTreeMap<ShapeId, Vec<Diff>> = BTreeMap::new();
	for (id, diff) in entries {
		grouped.entry(id).or_default().push(diff);
	}

	let mut result: Vec<Change> = Vec::with_capacity(grouped.len());
	for (id, diffs) in grouped {
		let coalesced = coalesce_inserts(diffs)?;
		result.push(Change {
			origin: ChangeOrigin::Shape(id),
			diffs: coalesced.into(),
			version,
			changed_at,
		});
	}
	Ok(result)
}

fn coalesce_inserts(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	let mut result: Vec<Diff> = Vec::with_capacity(diffs.len());
	let mut current_run: Vec<Arc<Columns>> = Vec::new();

	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
				..
			} => current_run.push(post),
			other => {
				flush_insert_run(&mut current_run, &mut result)?;
				result.push(other);
			}
		}
	}
	flush_insert_run(&mut current_run, &mut result)?;

	Ok(result)
}

fn flush_insert_run(run: &mut Vec<Arc<Columns>>, result: &mut Vec<Diff>) -> Result<()> {
	if run.is_empty() {
		return Ok(());
	}
	if run.len() == 1 {
		let only = run.pop().unwrap();
		result.push(Diff::insert_arc(only));
		return Ok(());
	}
	let mut iter = run.drain(..);
	let first = iter.next().unwrap();
	let mut merged = (*first).clone();
	for next in iter {
		merged.append_all((*next).clone())?;
	}
	result.push(Diff::insert_arc(Arc::new(merged)));
	Ok(())
}
