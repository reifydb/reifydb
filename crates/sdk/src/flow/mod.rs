//! Flow change types and builders

use reifydb_core::{
	CommitVersion,
	interface::{FlowNodeId, PrimitiveId},
	value::column::Columns,
};

pub mod builder;

pub use builder::FlowChangeBuilder;

/// Origin of a flow change
#[derive(Debug, Clone)]
pub enum FlowChangeOrigin {
	/// Change originated from an external source (table, view, ring buffer)
	External(PrimitiveId),
	/// Change originated from an internal flow node
	Internal(FlowNodeId),
}

/// Represents a single diff in a flow change (can contain 1 or more rows in columnar format)
#[derive(Debug, Clone)]
pub enum FlowDiff {
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

/// Represents a flow change with insertions, updates, and deletions
#[derive(Debug, Clone)]
pub struct FlowChange {
	/// Origin of this change
	pub origin: FlowChangeOrigin,
	/// The list of diffs (changes) in this flow change
	pub diffs: Vec<FlowDiff>,
	/// Version of this change
	pub version: CommitVersion,
}

impl FlowChange {
	/// Create a flow change from an external source
	pub fn external(source: PrimitiveId, version: CommitVersion, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::External(source),
			diffs,
			version,
		}
	}

	/// Create a flow change from an internal flow node
	pub fn internal(from: FlowNodeId, version: CommitVersion, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::Internal(from),
			diffs,
			version,
		}
	}
}
