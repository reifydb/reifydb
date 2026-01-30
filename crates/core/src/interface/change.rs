// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Change types for flow processing (columnar format)

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId},
	value::column::columns::Columns,
};

/// Origin of a change
#[derive(Debug, Clone)]
pub enum ChangeOrigin {
	/// Change originated from an external source (table, view, ring buffer)
	External(PrimitiveId),
	/// Change originated from an internal flow node
	Internal(FlowNodeId),
}

/// Represents a single diff (can contain 1 or more rows in columnar format)
#[derive(Debug, Clone)]
pub enum Diff {
	/// Insert new row(s)
	Insert {
		/// The row(s) to insert (columnar format, row_numbers tracked in Columns)
		post: Columns,
	},
	/// Update existing row(s)
	Update {
		/// The previous value(s)
		pre: Columns,
		/// The new value(s)
		post: Columns,
	},
	/// Remove existing row(s)
	Remove {
		/// The row(s) to remove
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
	/// Version of this change
	pub version: CommitVersion,
}

impl Change {
	/// Create a change from a primitive (external) source
	pub fn from_primitive(source: PrimitiveId, version: CommitVersion, diffs: Vec<Diff>) -> Self {
		Self {
			origin: ChangeOrigin::External(source),
			diffs,
			version,
		}
	}

	/// Create a change from a flow node (internal)
	pub fn from_flow(from: FlowNodeId, version: CommitVersion, diffs: Vec<Diff>) -> Self {
		Self {
			origin: ChangeOrigin::Internal(from),
			diffs,
			version,
		}
	}
}
