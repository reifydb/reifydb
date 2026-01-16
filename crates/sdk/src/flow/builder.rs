// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder for constructing FlowChange objects

use reifydb_core::{
	common::CommitVersion, interface::catalog::flow::FlowNodeId, row::Row, value::column::columns::Columns,
};

use super::{FlowChange, FlowDiff};

/// Builder for constructing FlowChange objects for internal flow operators
pub struct FlowChangeBuilder {
	operator_id: FlowNodeId,
	version: CommitVersion,
	diffs: Vec<FlowDiff>,
}

impl FlowChangeBuilder {
	/// Create a new FlowChangeBuilder for an internal operator
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
		self.diffs.push(FlowDiff::Insert {
			post,
		});
		self
	}

	/// Add an insert diff from a Row (converts to Columns)
	pub fn insert_row(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Insert {
			post: Columns::from_row(&row),
		});
		self
	}

	/// Add an update diff with Columns
	pub fn update(mut self, pre: Columns, post: Columns) -> Self {
		self.diffs.push(FlowDiff::Update {
			pre,
			post,
		});
		self
	}

	/// Add an update diff from Rows (converts to Columns)
	pub fn update_rows(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(FlowDiff::Update {
			pre: Columns::from_row(&pre),
			post: Columns::from_row(&post),
		});
		self
	}

	/// Add a remove diff with Columns
	pub fn remove(mut self, pre: Columns) -> Self {
		self.diffs.push(FlowDiff::Remove {
			pre,
		});
		self
	}

	/// Add a remove diff from a Row (converts to Columns)
	pub fn remove_row(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Remove {
			pre: Columns::from_row(&row),
		});
		self
	}

	/// Add a single diff
	pub fn diff(mut self, diff: FlowDiff) -> Self {
		self.diffs.push(diff);
		self
	}

	/// Add multiple diffs
	pub fn diffs(mut self, iter: impl IntoIterator<Item = FlowDiff>) -> Self {
		self.diffs.extend(iter);
		self
	}

	/// Build the FlowChange
	pub fn build(self) -> FlowChange {
		FlowChange::internal(self.operator_id, self.version, self.diffs)
	}
}
