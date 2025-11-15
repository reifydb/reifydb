// Redesigned operator traits with static metadata

use std::collections::HashMap;

use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_type::{RowNumber, Value};

use crate::{context::OperatorContext, error::Result};

/// Represents a flow change with insertions, updates, and deletions
#[derive(Debug, Clone)]
pub struct FlowChange {
	/// The list of diffs (changes) in this flow change
	pub diffs: Vec<FlowDiff>,
	/// Version of this change
	pub version: u64,
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
