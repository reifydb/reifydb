//! C ABI definitions for ReifyDB FFI operators
//!
//! This crate provides the stable C ABI interface that FFI operators must implement.
//! It defines FFI-safe types and function signatures for operators to interact with
//! the ReifyDB host system.

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod callbacks;
pub mod catalog;
pub mod constants;
pub mod context;
pub mod data;
pub mod flow;
pub mod operator;

pub use callbacks::{CatalogCallbacks, HostCallbacks, LogCallbacks, MemoryCallbacks, StateCallbacks, StoreCallbacks};
pub use catalog::{ColumnDefFFI, NamespaceFFI, PrimaryKeyFFI, TableFFI};
pub use constants::*;
pub use context::{ContextFFI, StateIteratorFFI, StoreIteratorFFI};
pub use data::{BufferFFI, ColumnDataFFI, ColumnFFI, ColumnTypeCode, ColumnsFFI, FieldFFI, LayoutFFI};
pub use flow::{FlowChangeFFI, FlowDiffFFI, FlowDiffType, FlowOriginFFI};
pub use operator::{
	CAPABILITY_ALL_STANDARD, CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_INSERT, CAPABILITY_PULL,
	CAPABILITY_TICK, CAPABILITY_UPDATE, OPERATOR_MAGIC, OperatorColumnDefFFI, OperatorColumnDefsFFI,
	OperatorCreateFnFFI, OperatorDescriptorFFI, OperatorMagicFnFFI, OperatorVTableFFI, has_capability,
};
