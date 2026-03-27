// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, schema::SchemaId},
	value::column::columns::Columns,
};

/// Origin of a change
#[derive(Debug, Clone)]
pub enum ChangeOrigin {
	Schema(SchemaId),
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

/// A change with origin, diffs, and version
#[derive(Debug, Clone)]
pub struct Change {
	/// Origin of this change
	pub origin: ChangeOrigin,
	/// The list of diffs (changes)
	pub diffs: Vec<Diff>,
	/// Version of this change.
	pub version: CommitVersion,
}

impl Change {
	/// Create a change from a schema (external) source
	pub fn from_schema(schema: SchemaId, version: CommitVersion, diffs: Vec<Diff>) -> Self {
		Self {
			origin: ChangeOrigin::Schema(schema),
			diffs,
			version,
		}
	}

	/// Create a change from a flow node (internal)
	pub fn from_flow(from: FlowNodeId, version: CommitVersion, diffs: Vec<Diff>) -> Self {
		Self {
			origin: ChangeOrigin::Flow(from),
			diffs,
			version,
		}
	}
}
