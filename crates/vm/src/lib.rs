// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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

pub mod builder;
pub mod bytecode;
pub mod compile;
pub mod dsl;
pub mod error;
pub mod expr;
pub mod operator;
pub mod pipeline;
pub mod source;
pub mod vmcore;

// Re-exports for convenience
pub use builder::PipelineBuilder;
pub use bytecode::{BytecodeReader, BytecodeWriter, Opcode, OperatorKind, Program};
pub use compile::BytecodeCompiler;
pub use dsl::{DslError, SourceRegistry, compile_script, execute_script, parse_pipeline};
pub use error::{Result, VmError};
pub use expr::{ColumnSchema, Expr, ExprBuilder, col, lit};
pub use pipeline::{Pipeline, collect};
pub use source::{InMemorySource, InMemorySourceRegistry, TableSource, from_batches, from_columns};
#[cfg(feature = "trace")]
pub use vmcore::{TraceEntry, VmTracer};
pub use vmcore::{VmConfig, VmContext, VmState};
