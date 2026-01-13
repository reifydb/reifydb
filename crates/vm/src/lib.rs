// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB VM - Vectorized query execution engine.
//!
//! This crate provides the core VM for executing queries in ReifyDB.
//! It operates on columnar data (`Columns`) using a streaming pipeline model.
//!
//! # Architecture
//!
//! The VM uses a pull-based streaming model where:
//! - **Sources** produce data (e.g., `InMemorySource`)
//! - **Operators** transform data (e.g., `FilterOp`, `SelectOp`, `TakeOp`)
//! - **Pipelines** are async streams of `Columns` batches
//!
//! # Example
//!
//! ```ignore
//! use reifydb_vm::{PipelineBuilder, col, lit};
//!
//! // Build a pipeline: scan -> filter -> select -> collect
//! let result = PipelineBuilder::from_columns(data)
//!     .filter(col("age").ge(lit(18)))
//!     .select_cols(&["name", "age"])
//!     .take(10)
//!     .collect()
//!     ?;
//! ```

pub mod error;
pub mod operator;
pub mod pipeline;
pub mod rql;

// VM runtime components
mod runtime;

// Handler modules (opcode handlers)
mod handler;

#[cfg(feature = "trace")]
pub mod trace;

// Re-exports for convenience
pub use runtime::builtin::BuiltinRegistry;
pub use runtime::stack::{CallFrame, CallStack};
pub use runtime::context::{VmConfig, VmContext};
pub use runtime::dispatch::DispatchResult;
pub use error::{Result, VmError};
pub use runtime::operand::{OperandValue, PipelineHandle, Record};
pub use operator::{ScanInlineOp, ScanTableOp};
pub use pipeline::{Pipeline, collect, empty, from_batches, from_columns, from_result};
pub use reifydb_rqlv2::bytecode::{BytecodeReader, BytecodeWriter, CompiledProgram, Opcode, OperatorKind};
pub use rql::execute_program;
pub use runtime::scope::{Scope, ScopeChain};
pub use runtime::script::BytecodeScriptCaller;
pub use runtime::state::VmState;
#[cfg(feature = "trace")]
pub use trace::{TraceEntry, VmTracer};
