// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	value::column::columns::Columns,
};

/// Origin of a change
#[derive(Debug, Clone)]
pub enum ChangeOrigin {
	Shape(ShapeId),
	Flow(FlowNodeId),
}
/// Represents a single diff
#[derive(Debug, Clone)]
pub enum Diff {
	Insert {
		post: Columns,
	},
	Update {
		pre: Columns,
		post: Columns,
	},
	Remove {
		pre: Columns,
	},
}

/// A change with origin, diffs, version, and timestamp
#[derive(Debug, Clone)]
pub struct Change {
	/// Origin of this change
	pub origin: ChangeOrigin,
	/// The list of diffs (changes)
	pub diffs: Vec<Diff>,
	/// Version of this change.
	pub version: CommitVersion,
	/// Timestamp when this was changed
	pub changed_at: DateTime,
}

impl Change {
	/// Create a change from a shape (external) source
	pub fn from_shape(shape: ShapeId, version: CommitVersion, diffs: Vec<Diff>, changed_at: DateTime) -> Self {
		Self {
			origin: ChangeOrigin::Shape(shape),
			diffs,
			version,
			changed_at,
		}
	}

	/// Create a change from a flow node (internal)
	pub fn from_flow(from: FlowNodeId, version: CommitVersion, diffs: Vec<Diff>, changed_at: DateTime) -> Self {
		Self {
			origin: ChangeOrigin::Flow(from),
			diffs,
			version,
			changed_at,
		}
	}
}
