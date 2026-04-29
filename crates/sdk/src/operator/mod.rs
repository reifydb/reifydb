// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Operator traits and types

use std::collections::HashMap;

use reifydb_type::value::{Value, row_number::RowNumber};

pub mod builder;
pub mod change;
pub mod column;
pub mod context;

use change::BorrowedChange;
use column::OperatorColumn;
use context::OperatorContext;
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_type::value::datetime::DateTime;

use crate::error::Result;

/// Static metadata about an operator type
/// This trait provides compile-time constant metadata
pub trait FFIOperatorMetadata {
	/// Operator name (must be unique within a library)
	const NAME: &'static str;
	/// API version for FFI compatibility (must match host's CURRENT_API)
	const API: u32;
	/// Semantic version of the operator (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description of the operator
	const DESCRIPTION: &'static str;
	/// Input columns describing expected input row format
	const INPUT_COLUMNS: &'static [OperatorColumn];
	/// Output columns describing output row format
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	/// Capabilities bitflags describing supported operations
	/// Use CAPABILITY_* constants from reifydb_abi
	const CAPABILITIES: u32;
}

/// Runtime operator behavior.
///
/// **Zero-copy ABI.** Input arrives as a `BorrowedChange<'_>` whose pointers
/// alias native column storage; the borrow checker pins it to the call
/// frame. Output is written via `ctx.builder()` - the operator acquires
/// host-pool-owned column buffers, fills them in place, and emits diffs
/// via `emit_insert` / `emit_update` / `emit_remove`. There is no owned
/// `Change` allocation in either direction.
///
/// Operators must be Send + Sync for thread safety.
pub trait FFIOperator: 'static {
	/// Create a new operator instance with the operator ID and configuration
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Process a flow change (inserts, updates, removes).
	///
	/// `input` borrows native column storage; do not retain pointers
	/// past return. Emit output diffs via `ctx.builder()`.
	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()>;

	/// Pull specific rows by row number. Emit the result as a single
	/// `Insert` diff via `ctx.builder()`; the host reads its `post`
	/// columns as the return value.
	fn pull(&mut self, ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<()>;

	/// Periodic tick for time-based maintenance (e.g., window eviction).
	/// If maintenance produced changes, emit them via `ctx.builder()` and
	/// return `Ok(true)`; otherwise return `Ok(false)`.
	fn tick(&mut self, _ctx: &mut OperatorContext, _timestamp: DateTime) -> Result<bool> {
		Ok(false)
	}

	/// Flush buffered state mutations.
	///
	/// Called once per txn at commit time. Operators that buffer state
	/// updates in a `StateCache` (or similar) override this to drain
	/// their pending dirty list and write each entry through the host's
	/// state callbacks. The default is a no-op for stateless operators.
	fn flush_state(&mut self, _ctx: &mut OperatorContext) -> Result<()> {
		Ok(())
	}
}

pub trait FFIOperatorWithMetadata: FFIOperator + FFIOperatorMetadata {}
impl<T> FFIOperatorWithMetadata for T where T: FFIOperator + FFIOperatorMetadata {}
