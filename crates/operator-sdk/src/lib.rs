//! ReifyDB Operator SDK

pub mod builders;
pub mod context;
pub mod error;
pub mod ffi;
pub mod macros;
pub mod operator;
pub mod patterns;
pub mod state;

// Re-export commonly used types
pub use builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder};
pub use context::OperatorContext;
pub use error::{Error, Result};
pub use operator::{Capabilities, FlowChange, FlowDiff, Operator, OperatorMetadata};
pub use state::State;

// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        __capability_method, assert_flow_change_eq, export_operator, flow_change, operator, row, test_operator,
    };

    pub use crate::builders::{FlowChangeBuilder, FlowChangeExt, RowBuilder};
    pub use crate::context::OperatorContext;
    pub use crate::error::{Error, Result};
    pub use crate::operator::{Capabilities, FlowChange, FlowDiff, Operator, OperatorMetadata};
    pub use crate::patterns::{stateless, StatelessOperator};
    pub use crate::state::State;

    pub use reifydb_core::Row;
    pub use reifydb_type::RowNumber;
    pub use reifydb_type::Value;
}
