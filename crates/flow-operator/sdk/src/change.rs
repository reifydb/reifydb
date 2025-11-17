//! Builder for constructing FlowChange objects

use reifydb_core::{CommitVersion, Row, interface::FlowNodeId};

use crate::{FlowChange, FlowDiff};

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

	/// Add an insert diff
	pub fn insert(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Insert {
			post: row,
		});
		self
	}

	/// Add an update diff
	pub fn update(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(FlowDiff::Update {
			pre,
			post,
		});
		self
	}

	/// Add a remove diff
	pub fn remove(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Remove {
			pre: row,
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
