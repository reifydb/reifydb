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
pub mod handler;
pub mod operator;
pub mod pipeline;
pub mod rql;
pub mod runtime;
#[cfg(feature = "trace")]
pub mod trace;
