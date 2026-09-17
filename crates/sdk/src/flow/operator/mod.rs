// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	operator_with::ApplyWith,
};
use reifydb_value::{config::ExtensionParams, value::duration::Duration};

use crate::{
	error::Result,
	flow::operator::{column::operator::OperatorColumn, context::GuestContext, timer::Timer, view::ChangeView},
};

pub trait OperatorMetadata {
	const NAME: &'static str;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: &'static [OperatorCapability];
}

pub trait GuestOperator: Send + Sync {
	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl GuestContext, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn seal_span(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}
