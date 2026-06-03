// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Operator authoring surface for SDK consumers. An operator is a node in a flow graph that takes a set of input
//! columns, transforms them, and produces output columns; this module exposes the builder, the column and row
//! views, the diff representation an operator emits, and the context that gives the operator access to engine
//! services. Anything an extension needs to write a useful operator lives here.

use crate::config::Config;

pub mod builder;
pub mod change;
pub mod column;
pub mod context;
pub mod diff;
pub mod view;
pub mod view_column;
pub mod windowed;

use change::BorrowedChange;
use column::operator::OperatorColumn;
use context::{OperatorContext, ffi::FFIOperatorContext};
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use view::ChangeView;

use crate::error::Result;

pub struct Tick {
	pub now: DateTime,
}

pub trait FFIOperator: 'static {
	fn new(operator_id: FlowNodeId, config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()>;

	fn tick(&mut self, _ctx: &mut FFIOperatorContext, _tick: Tick) -> Result<()> {
		Ok(())
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&mut self, _ctx: &mut FFIOperatorContext) -> Result<()> {
		Ok(())
	}
}

pub trait OperatorMetadata {
	const NAME: &'static str;
	const API: u32;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: &'static [OperatorCapability];
}

pub trait OperatorLogic: Send + Sync {
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()>;

	fn tick(&mut self, _ctx: &mut impl OperatorContext, _tick: Tick) -> Result<()> {
		Ok(())
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&mut self, _ctx: &mut impl OperatorContext) -> Result<()> {
		Ok(())
	}
}

pub struct FFIOperatorAdapter<C> {
	core: C,
}

impl<C: OperatorMetadata> OperatorMetadata for FFIOperatorAdapter<C> {
	const NAME: &'static str = C::NAME;
	const API: u32 = C::API;
	const VERSION: &'static str = C::VERSION;
	const DESCRIPTION: &'static str = C::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = C::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = C::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = C::CAPABILITIES;
}

impl<C: OperatorLogic + OperatorMetadata + 'static> FFIOperator for FFIOperatorAdapter<C> {
	fn new(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		Ok(Self {
			core: C::create(operator_id, config)?,
		})
	}

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		self.core.apply(ctx, input)
	}

	fn tick(&mut self, ctx: &mut FFIOperatorContext, tick: Tick) -> Result<()> {
		self.core.tick(ctx, tick)
	}

	fn ticks(&self) -> Option<Duration> {
		self.core.ticks()
	}

	fn flush_state(&mut self, ctx: &mut FFIOperatorContext) -> Result<()> {
		self.core.flush_state(ctx)
	}
}
