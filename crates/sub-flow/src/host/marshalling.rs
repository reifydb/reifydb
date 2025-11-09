//! Type marshalling between Rust and FFI types

use reifydb_operator_api::*;
use reifydb_core::{Row, interface::FlowNodeId};
use crate::flow::{FlowChange, FlowDiff};
use crate::host::Arena;

/// Marshaller for converting between Rust and FFI types
pub struct FFIMarshaller {
    arena: Arena,
}

impl FFIMarshaller {
    /// Create a new marshaller
    pub fn new() -> Self {
        Self {
            arena: Arena::new(),
        }
    }

    /// Marshal a flow change to FFI representation
    pub fn marshal_flow_change(&mut self, _change: &FlowChange) -> FlowChangeFFI {
        // TODO: Implement marshalling
        todo!()
    }

    /// Unmarshal a flow change from FFI representation
    pub fn unmarshal_flow_change(&self, _ffi: &FlowChangeFFI) -> crate::Result<FlowChange> {
        todo!()
    }

    /// Marshal a row to FFI representation
    pub fn marshal_row(&mut self, _row: &Row) -> *const RowFFI {
        todo!()
    }

    /// Unmarshal a row from FFI representation
    pub fn unmarshal_row(&self, _ffi: &RowFFI) -> Row {
        todo!()
    }

    /// Clear the arena
    pub fn clear(&mut self) {
        self.arena.clear();
    }
}

impl Default for FFIMarshaller {
    fn default() -> Self {
        Self::new()
    }
}