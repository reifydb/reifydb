// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator authoring surface: the builder, the column and row views, the diff an operator emits, and the context
//! through which it reaches engine services. Everything an extension needs to write an operator lives here.

use crate::config::Config;

pub mod builder;
pub mod change;
pub mod column;
pub mod context;
pub mod diff;
pub mod timer;
pub mod view;
pub mod view_column;
pub mod windowed;

use change::BorrowedChange;
use column::operator::OperatorColumn;
use context::{OperatorContext, ffi::FFIOperatorContext};
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::catalog::flow::OperatorId, key::operator_group_state::GroupSet, metrics::heap::OperatorSample,
};
use timer::Timer;
use view::ChangeView;

use crate::error::Result;

pub trait FFIOperator: 'static {
	fn new(operator_id: OperatorId, config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut FFIOperatorContext, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn invalidate_groups(&mut self, _groups: &GroupSet) {}

	fn seal_after_ms(&self) -> Option<u64> {
		None
	}

	fn flush_state(&mut self, _ctx: &mut FFIOperatorContext) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
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
	fn create(operator_id: OperatorId, config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl OperatorContext, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn seal_after_ms(&self) -> Option<u64> {
		None
	}

	fn flush_state(&mut self, _ctx: &mut impl OperatorContext) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn invalidate_groups(&mut self, _groups: &GroupSet) {}
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
	fn new(operator_id: OperatorId, config: &Config) -> Result<Self> {
		Ok(Self {
			core: C::create(operator_id, config)?,
		})
	}

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		self.core.apply(ctx, input)
	}

	fn on_timer(&mut self, ctx: &mut FFIOperatorContext, timer: Timer<'_>) -> Result<()> {
		self.core.on_timer(ctx, timer)
	}

	fn seal_after_ms(&self) -> Option<u64> {
		self.core.seal_after_ms()
	}

	fn flush_state(&mut self, ctx: &mut FFIOperatorContext) -> Result<()> {
		self.core.flush_state(ctx)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.core.sample()
	}

	fn invalidate_groups(&mut self, groups: &GroupSet) {
		self.core.invalidate_groups(groups)
	}
}
