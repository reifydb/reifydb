//! ReifyDB Operator SDK

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::collections::HashMap;

use reifydb_core::{
	CommitVersion, Row,
	interface::{FlowNodeId, SourceId},
};
use reifydb_type::{RowNumber, TypeConstraint, Value};

pub mod catalog;
pub mod change;
pub mod context;
pub mod error;
pub mod ffi;
pub mod marshal;
pub mod stateful;
pub mod store;
pub mod testing;

pub use catalog::Catalog;
pub use change::FlowChangeBuilder;
pub use context::OperatorContext;
pub use error::{FFIError, Result};
pub use reifydb_core::{
	CowVec,
	key::EncodableKey,
	value::encoded::{EncodedKey, EncodedValues},
};
pub use stateful::State;
pub use store::Store;

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

/// A single column definition in an operator's input/output
#[derive(Debug, Clone)]
pub struct OperatorColumnDef {
	/// Column name
	pub name: &'static str,
	/// Column type constraint (use TypeConstraint::unconstrained(Type::X) for unconstrained types)
	pub field_type: TypeConstraint,
	/// Human-readable description
	pub description: &'static str,
}

/// Static metadata about an operator type
/// This trait provides compile-time constant metadata
pub trait FFIOperatorMetadata {
	/// Operator name (must be unique within a library)
	const NAME: &'static str;
	/// API version for FFI compatibility (must match host's CURRENT_API)
	const API: u32;
	/// Semantic version of the operator (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description of the operator
	const DESCRIPTION: &'static str;
	/// Input columns describing expected input row format
	const INPUT_COLUMNS: &'static [OperatorColumnDef];
	/// Output columns describing output row format
	const OUTPUT_COLUMNS: &'static [OperatorColumnDef];
	/// Capabilities bitflags describing supported operations
	/// Use CAPABILITY_* constants from reifydb_flow_operator_abi
	const CAPABILITIES: u32;
}

/// Runtime operator behavior
/// Operators must be Send + Sync for thread safety
pub trait FFIOperator: Send + Sync + 'static {
	/// Create a new operator instance with the operator ID and configuration
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Process a flow change (inserts, updates, removes)
	fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange>;

	/// Get specific rows by row number
	fn get_rows(&mut self, ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>>;
}

pub trait FFIOperatorWithMetadata: FFIOperator + FFIOperatorMetadata {}
impl<T> FFIOperatorWithMetadata for T where T: FFIOperator + FFIOperatorMetadata {}

// Prelude module for convenient imports
pub mod prelude {
	pub use reifydb_core::{
		CowVec, Row,
		key::EncodableKey,
		value::encoded::{EncodedKey, EncodedValues},
	};
	pub use reifydb_flow_operator_abi::{
		CAPABILITY_ALL_STANDARD, CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_GET_ROWS, CAPABILITY_INSERT,
		CAPABILITY_TICK, CAPABILITY_UPDATE, has_capability,
	};
	pub use reifydb_type::{RowNumber, Type, TypeConstraint, Value};

	pub use crate::{
		Catalog, FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowChangeBuilder,
		FlowChangeOrigin, FlowDiff, OperatorColumnDef,
		context::OperatorContext,
		error::{FFIError, Result},
		stateful::State,
		store::Store,
	};
}
