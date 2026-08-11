// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator authoring surface: the builder, the column and row views, the diff an operator emits, and the context
//! through which it reaches engine services. Everything an extension needs to write an operator lives here.

pub mod change;
pub mod column;
pub mod context;
pub mod dictionary;
pub mod diff;
pub mod extern_c;
pub mod state;
pub mod timer;
pub mod view;
pub mod view_column;
pub mod windowed;

use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
use reifydb_value::{config::Config, value::duration::Duration};

use crate::{
	error::Result,
	flow::operator::{column::operator::OperatorColumn, context::OperatorContext, timer::Timer, view::ChangeView},
};

pub trait OperatorMetadata {
	const NAME: &'static str;
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

	fn seal_after(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&mut self, _ctx: &mut impl OperatorContext) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}
