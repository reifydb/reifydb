//! FFI bridge for operators

mod wrapper;
mod exports;

pub use wrapper::OperatorWrapper;
pub use exports::{create_descriptor, create_operator_instance};

// Re-export FFI types that operators might need
pub use reifydb_operator_abi::{
    FFIOperatorDescriptor,
    FFIOperatorVTable,
    TransactionHandle,
    FlowChangeFFI,
    RowsFFI,
};