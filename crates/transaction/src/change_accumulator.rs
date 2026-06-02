// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem};

use indexmap::IndexMap;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::shape::ShapeId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

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
		let coalesced = coalesce_diffs(diffs)?;
		if coalesced.is_empty() {
			continue;
		}
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
	let mut current_run: Vec<Columns> = Vec::new();

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

fn flush_insert_run(run: &mut Vec<Columns>, result: &mut Vec<Diff>) -> Result<()> {
	if run.is_empty() {
		return Ok(());
	}
	if run.len() == 1 {
		let only = run.pop().unwrap();
		result.push(Diff::insert(only));
		return Ok(());
	}
	let mut iter = run.drain(..);
	let mut merged = iter.next().unwrap();
	for next in iter {
		merged.append_all(next)?;
	}
	result.push(Diff::insert(merged));
	Ok(())
}

enum RowState {
	Inserted {
		post: Columns,
	},
	Updated {
		pre: Columns,
		post: Columns,
	},
	Removed {
		pre: Columns,
	},
}

fn coalesce_diffs(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	if !diffs.iter().all(diff_is_row_keyed) {
		return coalesce_inserts(diffs);
	}

	let mut states: IndexMap<RowNumber, RowState> = IndexMap::new();
	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				for i in 0..post.row_count() {
					apply_insert(&mut states, post.row_numbers[i], post.extract_row(i));
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				for i in 0..post.row_count() {
					apply_update(
						&mut states,
						post.row_numbers[i],
						pre.extract_row(i),
						post.extract_row(i),
					);
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				for i in 0..pre.row_count() {
					apply_remove(&mut states, pre.row_numbers[i], pre.extract_row(i));
				}
			}
		}
	}

	let mut inserts: Option<Columns> = None;
	let mut update_pre: Option<Columns> = None;
	let mut update_post: Option<Columns> = None;
	let mut removes: Option<Columns> = None;

	for (_, state) in states {
		match state {
			RowState::Inserted {
				post,
			} => append_into(&mut inserts, post)?,
			RowState::Updated {
				pre,
				post,
			} => {
				append_into(&mut update_pre, pre)?;
				append_into(&mut update_post, post)?;
			}
			RowState::Removed {
				pre,
			} => append_into(&mut removes, pre)?,
		}
	}

	let mut result: Vec<Diff> = Vec::with_capacity(3);
	if let Some(post) = inserts {
		result.push(Diff::insert(post));
	}
	if let (Some(pre), Some(post)) = (update_pre, update_post) {
		result.push(Diff::update(pre, post));
	}
	if let Some(pre) = removes {
		result.push(Diff::remove(pre));
	}
	Ok(result)
}

fn diff_is_row_keyed(diff: &Diff) -> bool {
	match diff {
		Diff::Insert {
			post,
			..
		} => columns_row_keyed(post),
		Diff::Update {
			pre,
			post,
			..
		} => columns_row_keyed(pre) && columns_row_keyed(post) && pre.row_count() == post.row_count(),
		Diff::Remove {
			pre,
			..
		} => columns_row_keyed(pre),
	}
}

fn columns_row_keyed(columns: &Columns) -> bool {
	columns.row_count() > 0 && columns.row_numbers.len() == columns.row_count()
}

fn apply_insert(states: &mut IndexMap<RowNumber, RowState>, row: RowNumber, post: Columns) {
	let next = match states.get(&row) {
		Some(RowState::Updated {
			pre,
			..
		})
		| Some(RowState::Removed {
			pre,
		}) => RowState::Updated {
			pre: pre.clone(),
			post,
		},
		_ => RowState::Inserted {
			post,
		},
	};
	states.insert(row, next);
}

fn apply_update(states: &mut IndexMap<RowNumber, RowState>, row: RowNumber, pre: Columns, post: Columns) {
	let next = match states.get(&row) {
		None => RowState::Updated {
			pre,
			post,
		},
		Some(RowState::Inserted {
			..
		}) => RowState::Inserted {
			post,
		},
		Some(RowState::Updated {
			pre: pre0,
			..
		})
		| Some(RowState::Removed {
			pre: pre0,
		}) => RowState::Updated {
			pre: pre0.clone(),
			post,
		},
	};
	states.insert(row, next);
}

fn apply_remove(states: &mut IndexMap<RowNumber, RowState>, row: RowNumber, pre: Columns) {
	match states.get(&row) {
		None => {
			states.insert(
				row,
				RowState::Removed {
					pre,
				},
			);
		}
		Some(RowState::Inserted {
			..
		}) => {
			states.shift_remove(&row);
		}
		Some(RowState::Updated {
			pre: pre0,
			..
		}) => {
			let pre0 = pre0.clone();
			states.insert(
				row,
				RowState::Removed {
					pre: pre0,
				},
			);
		}
		Some(RowState::Removed {
			..
		}) => {}
	}
}

fn append_into(target: &mut Option<Columns>, source: Columns) -> Result<()> {
	match target {
		Some(existing) => existing.append_all(source),
		None => {
			*target = Some(source);
			Ok(())
		}
	}
}
