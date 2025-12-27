//! FFI bridge for operators

mod arena;
pub mod exports;
mod wrapper;

pub use arena::Arena;
pub use exports::{create_descriptor, create_operator_instance, operator_magic};
// Re-export FFI types that operators might need
pub use reifydb_abi::{ContextFFI, FlowChangeFFI, OperatorDescriptorFFI, OperatorVTableFFI};
pub use wrapper::OperatorWrapper;
