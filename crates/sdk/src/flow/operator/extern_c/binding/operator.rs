// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
use reifydb_value::{config::Config, value::duration::Duration};

use crate::{
	error::Result,
	flow::operator::{
		GuestOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
		extern_c::binding::context::ExternCContext, timer::Timer,
	},
};

pub trait ExternCOperator: 'static {
	fn new(operator_id: OperatorId, config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut ExternCContext, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn seal_after(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub struct ExternCOperatorAdapter<C> {
	core: C,
}

impl<C: OperatorMetadata> OperatorMetadata for ExternCOperatorAdapter<C> {
	const NAME: &'static str = C::NAME;
	const VERSION: &'static str = C::VERSION;
	const DESCRIPTION: &'static str = C::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = C::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = C::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = C::CAPABILITIES;
}

impl<C: GuestOperator + OperatorMetadata + 'static> ExternCOperator for ExternCOperatorAdapter<C> {
	fn new(operator_id: OperatorId, config: &Config) -> Result<Self> {
		Ok(Self {
			core: C::create(operator_id, config)?,
		})
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
		self.core.apply(ctx, input)
	}

	fn on_timer(&mut self, ctx: &mut ExternCContext, timer: Timer<'_>) -> Result<()> {
		self.core.on_timer(ctx, timer)
	}

	fn seal_after(&self) -> Option<Duration> {
		self.core.seal_after()
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.core.sample()
	}
}
