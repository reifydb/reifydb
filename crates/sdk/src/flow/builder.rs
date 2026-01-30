// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder for constructing Change objects

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	row::Row,
	value::column::columns::Columns,
};

/// Builder for constructing Change objects for internal flow operators
pub struct ChangeBuilder {
	operator_id: FlowNodeId,
	version: CommitVersion,
	diffs: Vec<Diff>,
}

impl ChangeBuilder {
	/// Create a new ChangeBuilder for an internal operator
	///
	/// # Arguments
	/// * `operator_id` - The ID of the operator creating this change
	/// * `version` - The commit version for this change
	pub fn new(operator_id: FlowNodeId, version: CommitVersion) -> Self {
		Self {
			operator_id,
			version,
			diffs: Vec::new(),
		}
	}

	/// Add an insert diff with Columns
	pub fn insert(mut self, post: Columns) -> Self {
		self.diffs.push(Diff::Insert {
			post,
		});
		self
	}

	/// Add an insert diff from a Row (converts to Columns)
	pub fn insert_row(mut self, row: Row) -> Self {
		self.diffs.push(Diff::Insert {
			post: Columns::from_row(&row),
		});
		self
	}

	/// Add an update diff with Columns
	pub fn update(mut self, pre: Columns, post: Columns) -> Self {
		self.diffs.push(Diff::Update {
			pre,
			post,
		});
		self
	}

	/// Add an update diff from Rows (converts to Columns)
	pub fn update_rows(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(Diff::Update {
			pre: Columns::from_row(&pre),
			post: Columns::from_row(&post),
		});
		self
	}

	/// Add a remove diff with Columns
	pub fn remove(mut self, pre: Columns) -> Self {
		self.diffs.push(Diff::Remove {
			pre,
		});
		self
	}

	/// Add a remove diff from a Row (converts to Columns)
	pub fn remove_row(mut self, row: Row) -> Self {
		self.diffs.push(Diff::Remove {
			pre: Columns::from_row(&row),
		});
		self
	}

	/// Add a single diff
	pub fn diff(mut self, diff: Diff) -> Self {
		self.diffs.push(diff);
		self
	}

	/// Add multiple diffs
	pub fn diffs(mut self, iter: impl IntoIterator<Item = Diff>) -> Self {
		self.diffs.extend(iter);
		self
	}

	/// Build the Change
	pub fn build(self) -> Change {
		Change::from_flow(self.operator_id, self.version, self.diffs)
	}
}
