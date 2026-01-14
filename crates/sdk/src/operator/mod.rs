// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator traits and types

use std::collections::HashMap;

use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_type::{RowNumber, Value};

use crate::{FlowChange, Result};

pub mod column;
pub mod context;

pub use column::OperatorColumnDef;
pub use context::OperatorContext;

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
	const INPUT_COLUMNS: &'static [OperatorColumnDef];
	/// Output columns describing output row format
	const OUTPUT_COLUMNS: &'static [OperatorColumnDef];
	/// Capabilities bitflags describing supported operations
	/// Use CAPABILITY_* constants from reifydb_abi
	const CAPABILITIES: u32;
}

/// Runtime operator behavior
/// Operators must be Send + Sync for thread safety
pub trait FFIOperator: 'static {
	/// Create a new operator instance with the operator ID and configuration
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Process a flow change (inserts, updates, removes)
	fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange>;

	/// Pull specific rows by row number (returns Columns containing found rows)
	fn pull(&mut self, ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Columns>;
}

pub trait FFIOperatorWithMetadata: FFIOperator + FFIOperatorMetadata {}
impl<T> FFIOperatorWithMetadata for T where T: FFIOperator + FFIOperatorMetadata {}
