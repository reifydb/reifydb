// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
use reifydb_core::{interface::catalog::flow::FlowNodeId, row::Ttl};
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

/// Tick context passed to operators on each periodic firing.
///
/// Plain struct (not enum) so it maps directly onto the FFI vtable's
/// `tick(instance, ctx, timestamp_nanos: u64)` signature - the host wraps
/// `Tick { now }` and unwraps it at the FFI boundary, no discriminant
/// negotiation across the C ABI.
pub struct Tick {
	pub now: DateTime,
}

pub trait FFIOperator: 'static {
	/// Construct an operator instance.
	///
	/// `ttl` carries the operator-state TTL configured via `WITH { ttl: { ... } }`
	/// in the flow DDL. `None` means no eviction (the absent-clause default;
	/// state grows unbounded). When `Some`, `cleanup_mode` is always `Drop` -
	/// operator-state cleanup is silent by design.
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>, ttl: Option<Ttl>) -> Result<Self>
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
