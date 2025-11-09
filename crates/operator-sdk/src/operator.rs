//! Core operator trait and metadata

use crate::error::Result;
use crate::context::OperatorContext;
use reifydb_core::Row;
use reifydb_type::RowNumber;

/// Flow change type (simplified for SDK)
#[derive(Debug, Clone)]
pub struct FlowChange {
    pub diffs: Vec<FlowDiff>,
    pub version: u64,
}

/// Flow diff type
#[derive(Debug, Clone)]
pub enum FlowDiff {
    Insert { post: Row },
    Update { pre: Row, post: Row },
    Remove { pre: Row },
}

/// Core operator trait that all operators must implement
pub trait Operator: Send + Sync + 'static {
    /// Initialize the operator with configuration
    fn initialize(&mut self, _config: &[u8]) -> Result<()> {
        Ok(()) // Default no-op
    }

    /// Core apply logic - must be implemented
    fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange>;

    /// Get rows by row numbers
    fn get_rows(&mut self, _ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>> {
        // Default implementation returns None for all rows
        Ok(vec![None; row_numbers.len()])
    }

    /// Called when operator is destroyed
    fn destroy(&mut self) {
        // Default no-op
    }

    /// Operator metadata
    fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata::default()
    }
}

/// Operator metadata
#[derive(Debug, Clone)]
pub struct OperatorMetadata {
    pub name: &'static str,
    pub version: u32,
    pub capabilities: Capabilities,
}

impl Default for OperatorMetadata {
    fn default() -> Self {
        Self {
            name: "unknown",
            version: 1,
            capabilities: Capabilities::default(),
        }
    }
}

/// Operator capabilities flags
#[derive(Debug, Clone, Copy, Default)]
pub struct Capabilities {
    pub stateful: bool,
    pub keyed: bool,
    pub windowed: bool,
    pub batch: bool,
}

impl Capabilities {
    /// Create new capabilities with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set stateful capability
    pub fn with_stateful(mut self, value: bool) -> Self {
        self.stateful = value;
        self
    }

    /// Builder method to set keyed capability
    pub fn with_keyed(mut self, value: bool) -> Self {
        self.keyed = value;
        self
    }

    /// Builder method to set windowed capability
    pub fn with_windowed(mut self, value: bool) -> Self {
        self.windowed = value;
        self
    }

    /// Builder method to set batch capability
    pub fn with_batch(mut self, value: bool) -> Self {
        self.batch = value;
        self
    }

    /// Convert to FFI capability flags
    pub fn to_ffi_flags(&self) -> u32 {
        let mut flags = 0;
        if self.stateful {
            flags |= reifydb_operator_api::CAP_USES_STATE;
        }
        if self.keyed {
            flags |= reifydb_operator_api::CAP_KEYED_STATE;
        }
        if self.windowed {
            flags |= reifydb_operator_api::CAP_WINDOWED;
        }
        if self.batch {
            flags |= reifydb_operator_api::CAP_BATCH;
        }
        flags
    }
}