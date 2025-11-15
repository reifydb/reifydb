//! ReifyDB Operator SDK

use std::collections::HashMap;

use reifydb_core::{
	CommitVersion, Row,
	interface::{FlowNodeId, SourceId},
};
use reifydb_type::{RowNumber, Value};

pub mod builders;
pub mod context;
pub mod error;
pub mod ffi;
pub mod marshal;
pub mod state;

// Re-export commonly used types
pub use builders::{FlowChangeBuilder, RowBuilder};
pub use context::OperatorContext;
pub use error::{FFIError, Result};
pub use state::State;

/// Origin of a flow change
#[derive(Debug, Clone)]
pub enum FlowChangeOrigin {
	/// Change originated from an external source (table, view, ring buffer)
	External(SourceId),
	/// Change originated from an internal flow node
	Internal(FlowNodeId),
}

/// Represents a single diff in a flow change
#[derive(Debug, Clone)]
pub enum FlowDiff {
	/// Insert a new row
	Insert {
		/// The row to insert
		post: Row,
	},
	/// Update an existing row
	Update {
		/// The previous value
		pre: Row,
		/// The new value
		post: Row,
	},
	/// Remove an existing row
	Remove {
		/// The row to remove
		pre: Row,
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
	pub fn external(source: SourceId, version: CommitVersion, diffs: Vec<FlowDiff>) -> Self {
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

/// Static metadata about an operator type
/// This trait provides compile-time constant metadata
pub trait FFIOperatorMetadata {
	/// Operator name (must be unique within a library)
	const NAME: &'static str;
	/// Operator version
	const VERSION: u32;
}

/// Runtime operator behavior
/// Operators must be Send + Sync for thread safety
pub trait FFIOperator: Send + Sync + 'static {
	/// Create a new operator instance with the operator ID and configuration
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Get the operator ID for this instance
	fn operator_id(&self) -> FlowNodeId;

	/// Process a flow change (inserts, updates, removes)
	fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange>;

	/// Get specific rows by row number
	fn get_rows(&mut self, ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>>;

	/// Clean up resources before the operator is destroyed
	/// Default implementation does nothing
	fn destroy(&mut self) {
		// Optional cleanup
	}
}

/// Combined trait for FFI-exportable operators
/// Implement both FFIOperatorMetadata and FFIOperator to make an operator exportable
pub trait FFIOperatorWithMetadata: FFIOperator + FFIOperatorMetadata {}

// Blanket implementation - any type implementing both traits is FFI-exportable
impl<T> FFIOperatorWithMetadata for T where T: FFIOperator + FFIOperatorMetadata {}

// Prelude module for convenient imports
pub mod prelude {
	pub use reifydb_core::Row;
	pub use reifydb_type::{RowNumber, Value};

	pub use crate::{
		FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowChangeOrigin, FlowDiff,
		builders::{FlowChangeBuilder, RowBuilder},
		context::OperatorContext,
		error::{FFIError, Result},
		state::State,
	};
}
