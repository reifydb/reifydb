// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::{util::cowvec::CowVec, value::datetime::DateTime};

pub struct ChangeBuilder {
	operator_id: FlowNodeId,
	version: CommitVersion,
	diffs: Diffs,
	changed_at: DateTime,
}

impl ChangeBuilder {
	pub fn new(operator_id: FlowNodeId, version: CommitVersion) -> Self {
		Self {
			operator_id,
			version,
			diffs: Diffs::new(),
			changed_at: DateTime::default(),
		}
	}

	pub fn changed_at(mut self, changed_at: DateTime) -> Self {
		self.changed_at = changed_at;
		self
	}

	pub fn insert(mut self, post: Columns) -> Self {
		self.diffs.push(Diff::insert(post));
		self
	}

	pub fn insert_row(mut self, row: Row) -> Self {
		self.diffs.push(Diff::insert(Columns::from_row(&row)));
		self
	}

	pub fn update(mut self, pre: Columns, post: Columns) -> Self {
		self.diffs.push(Diff::update(pre, post));
		self
	}

	pub fn update_rows(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
		self
	}

	pub fn remove(mut self, pre: Columns) -> Self {
		self.diffs.push(Diff::remove(pre));
		self
	}

	pub fn remove_row(mut self, row: Row) -> Self {
		self.diffs.push(Diff::remove(Columns::from_row(&row)));
		self
	}

	pub fn diff(mut self, diff: Diff) -> Self {
		self.diffs.push(diff);
		self
	}

	pub fn diffs(mut self, iter: impl IntoIterator<Item = Diff>) -> Self {
		self.diffs.extend(iter);
		self
	}

	pub fn build(self) -> Change {
		let timestamp = self.changed_at;
		let diffs: Diffs = self
			.diffs
			.into_iter()
			.map(|diff| match diff {
				Diff::Insert {
					post,
				} => Diff::insert(Self::ensure_timestamps((*post).clone(), timestamp)),
				Diff::Update {
					pre,
					post,
				} => Diff::update(
					Self::ensure_timestamps((*pre).clone(), timestamp),
					Self::ensure_timestamps((*post).clone(), timestamp),
				),
				Diff::Remove {
					pre,
				} => Diff::remove(Self::ensure_timestamps((*pre).clone(), timestamp)),
			})
			.collect();
		Change::from_flow(self.operator_id, self.version, diffs, self.changed_at)
	}

	fn ensure_timestamps(columns: Columns, timestamp: DateTime) -> Columns {
		let row_count = columns.row_count();
		if row_count > 0 && columns.created_at.is_empty() {
			Columns {
				created_at: CowVec::new(vec![timestamp; row_count]),
				updated_at: CowVec::new(vec![timestamp; row_count]),
				..columns
			}
		} else {
			columns
		}
	}
}
