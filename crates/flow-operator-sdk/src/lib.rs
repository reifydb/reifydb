//! ReifyDB Operator SDK

pub mod builders;
pub mod context;
pub mod error;
pub mod ffi;
pub mod macros;
pub mod operator;
pub mod state;

// Re-export commonly used types
pub use builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder};
pub use context::OperatorContext;
pub use error::{Error, Result};
pub use operator::{Capabilities, FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowDiff};
pub use state::State;

// Prelude module for convenient imports
pub mod prelude {
	pub use reifydb_core::Row;
	pub use reifydb_type::{RowNumber, Value};

	pub use crate::{
		__capability_method, assert_flow_change_eq,
		builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder},
		context::OperatorContext,
		error::{Error, Result},
		export_operator, flow_change, operator,
		operator::{
			Capabilities, FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowDiff,
		},
		row,
		state::State,
		test_operator,
	};
}
