//! ReifyDB Operator SDK

pub mod builders;
pub mod context;
pub mod error;
pub mod ffi;
pub mod marshal;
pub mod operator;
pub mod state;

// Re-export commonly used types
pub use builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder};
pub use context::OperatorContext;
pub use error::{Error, Result};
pub use operator::{FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowDiff};
pub use state::State;

// Prelude module for convenient imports
pub mod prelude {
	pub use reifydb_core::Row;
	pub use reifydb_type::{RowNumber, Value};

	pub use crate::{
		FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, FlowChange, FlowDiff,
		builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder},
		context::OperatorContext,
		error::{Error, Result},
		operator,
		state::State,
	};
}
