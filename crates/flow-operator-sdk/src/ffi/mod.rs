//! FFI bridge for operators

mod arena;
pub mod exports;
mod marshaller;
mod wrapper;

pub use arena::Arena;
pub use exports::{create_descriptor, create_operator_instance};
pub use marshaller::FFIMarshaller;
// Re-export FFI types that operators might need
pub use reifydb_flow_operator_abi::{
	FFIOperatorDescriptor, FFIOperatorVTable, FlowChangeFFI, RowsFFI, TransactionHandle,
};
pub use wrapper::OperatorWrapper;
