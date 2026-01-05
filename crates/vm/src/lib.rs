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
//!     .await?;
//! ```

pub mod error;
pub mod operator;
pub mod pipeline;
pub mod rql;
pub mod vmcore;

// Re-exports for convenience
pub use error::{Result, VmError};
pub use operator::{ScanInlineOp, ScanTableOp};
pub use pipeline::{Pipeline, collect, empty, from_batches, from_columns, from_result};
pub use reifydb_rqlv2::bytecode::{
	BytecodeReader,
	BytecodeWriter,
	CompiledProgram as Program, // Alias for API compatibility
	Opcode,
	OperatorKind,
};
pub use rql::{RqlError, compile_script, execute_program};
#[cfg(feature = "trace")]
pub use vmcore::{TraceEntry, VmTracer};
pub use vmcore::{VmConfig, VmContext, VmState};
