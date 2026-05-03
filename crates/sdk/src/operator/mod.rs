// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Operator authoring surface for SDK consumers. An operator is a node in a flow graph that takes a set of input
//! columns, transforms them, and produces output columns; this module exposes the builder, the column and row
//! views, the diff representation an operator emits, and the context that gives the operator access to engine
//! services. Anything an extension needs to write a useful operator lives here.

use std::collections::HashMap;

use reifydb_type::value::{Value, row_number::RowNumber};

pub mod builder;
pub mod change;
pub mod column;
pub mod context;
pub mod diff;
pub mod view_column;
pub mod view_row;

use change::BorrowedChange;
use column::OperatorColumn;
use context::OperatorContext;
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_type::value::datetime::DateTime;

use crate::error::Result;

pub trait FFIOperatorMetadata {
	const NAME: &'static str;
	const API: u32;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: u32;
}

pub struct Tick {
	pub now: DateTime,
}

pub trait FFIOperator: 'static {
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()>;

	fn pull(&mut self, ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<()>;

	fn tick(&mut self, _ctx: &mut OperatorContext, _tick: Tick) -> Result<bool> {
		Ok(false)
	}

	fn flush_state(&mut self, _ctx: &mut OperatorContext) -> Result<()> {
		Ok(())
	}
}

pub trait FFIOperatorWithMetadata: FFIOperator + FFIOperatorMetadata {}
impl<T> FFIOperatorWithMetadata for T where T: FFIOperator + FFIOperatorMetadata {}
