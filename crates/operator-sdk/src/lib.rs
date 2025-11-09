//! ReifyDB Operator SDK
//!
//! This SDK provides a high-level Rust API for developing FFI operators
//! that can be loaded dynamically into ReifyDB's flow processing system.
//!
//! # Example
//!
//! ```rust
//! use reifydb_operator_sdk::prelude::*;
//!
//! #[derive(Default)]
//! struct MyOperator {
//!     count: u64,
//! }
//!
//! impl Operator for MyOperator {
//!     fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
//!         // Process input and return output
//!         self.count += input.diff_count() as u64;
//!
//!         // Store count in state
//!         ctx.state().set("count", &self.count)?;
//!
//!         Ok(input) // Pass through
//!     }
//! }
//!
//! // Export the operator for FFI
//! export_operator!(MyOperator);
//! ```

pub mod error;
pub mod operator;
pub mod context;
pub mod state;
pub mod builders;
pub mod macros;
pub mod ffi;
pub mod patterns;
pub mod testing;

// Re-export commonly used types
pub use error::{Error, Result};
pub use operator::{Operator, OperatorMetadata, Capabilities, FlowChange, FlowDiff};
pub use context::{OperatorContext, MockContext};
pub use state::State;
pub use builders::{FlowChangeBuilder, RowBuilder, FlowChangeExt};

// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        export_operator,
        operator,
        flow_change,
        row,
        test_operator,
        assert_flow_change_eq,
        __capability_method,
    };

    pub use crate::error::{Error, Result};
    pub use crate::operator::{Operator, OperatorMetadata, Capabilities, FlowChange, FlowDiff};
    pub use crate::context::{OperatorContext, MockContext};
    pub use crate::state::State;
    pub use crate::builders::{FlowChangeBuilder, RowBuilder, FlowChangeExt};
    pub use crate::patterns::{StatelessOperator, stateless};

    pub use reifydb_core::Row;
    pub use reifydb_type::Value;
    pub use reifydb_type::RowNumber;
    pub use serde_json::json;
}