// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_core::{
	common::{OnEmpty, WindowKind, WindowSize},
	expression::Expression,
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	internal_err,
	metrics::heap::OperatorSample,
	state::timer::TimerKind,
};
use reifydb_flow::{aggregate::AggregateContext, context::FlowContext, operator::source::decode_change};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{
		datetime::DateTime,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{require_row_numbers, require_time},
	},
};

use super::{
	apply::{
		apply_session_engine, apply_tumbling_engine, forget_sealed_groups, seal_engine_windows,
		seal_session_engines,
	},
	rolling::{BuiltinRolling, apply_rolling_engine, seal_rolling_engine},
};
use crate::{
	operator::{
		HostOperator, TimedOutput,
		aggregation::core::Aggregation,
		host::HostContext,
		state::seal::{ledger::FiredAt, rule::SealRule},
	},
	timer::Timer,
	window::{
		coord::DateTimeCoord, driver::runner::WindowRunner, engine::config::WindowEngineConfig,
		kind::sliding::fits_slide,
	},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

pub struct WindowConfig {
	pub parent_schema: Option<SchemaRef>,
	pub operator: OperatorId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
	pub ctx: Arc<FlowContext>,
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,

	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
	rolling: Option<Box<BuiltinRolling>>,
	pub(super) runner: Option<WindowRunner<DateTimeCoord>>,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Result<Self> {
		let core = Aggregation::new(
			config.operator,
			config.parent_schema,
			config.group_by,
			config.aggregations,
			config.routines,
			config.runtime_context,
			AggregateContext::Windowed,
			config.ctx,
		)?;
		if config.kind.size().is_some_and(WindowSize::is_count) {
			return internal_err!("a native window must be sized by duration");
		}
		if let WindowKind::Sliding {
			size,
			slide,
			..
		} = &config.kind
		{
			let fits = match (size, slide) {
				(WindowSize::Duration(size), WindowSize::Duration(slide)) => fits_slide(*size, *slide),
				_ => false,
			};
			if !fits {
				return internal_err!(
					"a native sliding window needs a duration slide above zero and below its size"
				);
			}
		}
		let lateness = config.lateness.unwrap_or_else(Duration::zero);
		let runner = match &config.kind {
			WindowKind::Tumbling {
				size: WindowSize::Duration(size),
			} => Some(WindowRunner::new(
				config.operator,
				*size,
				None,
				SealRule::for_window(&config.kind, lateness).map(|rule| rule.seal_span().duration()),
				&WindowEngineConfig::builder().build(),
			)),
			WindowKind::Sliding {
				size: WindowSize::Duration(size),
				slide: WindowSize::Duration(slide),
				..
			} => Some(WindowRunner::new(
				config.operator,
				*size,
				Some(*slide),
				SealRule::for_window(&config.kind, lateness).map(|rule| rule.seal_span().duration()),
				&WindowEngineConfig::builder().build(),
			)),
			WindowKind::Session {
				gap,
			} => Some(WindowRunner::sessions(
				config.operator,
				*gap,
				SealRule::for_window(&config.kind, lateness).map(|rule| rule.seal_span().duration()),
				&WindowEngineConfig::builder().build(),
			)),
			_ => None,
		};
		Ok(Self {
			core,
			kind: config.kind,
			lateness: config.lateness,
			immutable: config.immutable,
			rolling: None,
			runner,
		})
	}

	pub(crate) fn rolling_slot(&mut self) -> &mut Option<Box<BuiltinRolling>> {
		&mut self.rolling
	}

	pub(crate) fn engine_config(&self) -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	pub fn lateness(&self) -> Option<Duration> {
		self.lateness
	}

	pub fn immutable(&self) -> Option<Duration> {
		self.immutable
	}

	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	pub fn rolling_pane(&self) -> Option<Duration> {
		match &self.kind {
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
				pane: Some(pane),
				..
			} if !pane.is_zero() => Some(*pane),
			_ => None,
		}
	}

	pub fn rolling_on_empty(&self) -> OnEmpty {
		match &self.kind {
			WindowKind::Rolling {
				on_empty,
				..
			} => *on_empty,
			_ => OnEmpty::Remove,
		}
	}

	pub fn rolling_lag(&self) -> Duration {
		match &self.kind {
			WindowKind::Rolling {
				lag: Some(lag),
				..
			} => *lag,
			_ => Duration::default(),
		}
	}

	pub fn row_times(&self, columns: &RecordBatch, row_count: usize) -> Result<Vec<DateTime>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		Ok(require_time(columns)?.to_vec())
	}
}

pub(crate) fn required_row_numbers(columns: &RecordBatch) -> Result<&[RowNumber]> {
	if columns.num_rows() == 0 {
		return Ok(&[]);
	}
	require_row_numbers(columns)
}

impl HostOperator for WindowOperator {
	fn id(&self) -> OperatorId {
		self.core.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let change = decode_change(host, change)?;
		let out = match self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_engine(self, host, change),
			WindowKind::Sliding {
				..
			} => apply_tumbling_engine(self, host, change),
			WindowKind::Rolling {
				..
			} => apply_rolling_engine(self, host, change),
			WindowKind::Session {
				..
			} => apply_session_engine(self, host, change),
		}?;
		Ok(out)
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<TimedOutput>> {
		let fired = FiredAt::of(&timer);
		if timer.kind == TimerKind::Maintenance {
			forget_sealed_groups(self, host, fired)?;
			return Ok(None);
		}
		let diffs = match self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			} => seal_engine_windows(self, host, fired)?,
			WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => seal_rolling_engine(self, host, fired, timer.kind)?,
			WindowKind::Session {
				..
			} => seal_session_engines(self, host, slice::from_ref(&timer))?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(TimedOutput::new(diffs, timer.due)))
		}
	}

	fn batches_timers(&self) -> bool {
		matches!(self.kind, WindowKind::Session { .. })
	}

	fn on_timers(&mut self, host: &mut dyn HostContext, timers: Vec<Timer>) -> Result<Vec<Option<TimedOutput>>> {
		if !self.batches_timers() || timers.iter().any(|timer| timer.kind != TimerKind::Seal) {
			return internal_err!("operator {} batches only session seal timers", self.core.operator.0);
		}
		let diffs = seal_session_engines(self, host, &timers)?;
		let mut outputs: Vec<Option<TimedOutput>> = timers.iter().map(|_| None).collect();
		if !diffs.is_empty()
			&& let (Some(slot), Some(latest)) =
				(outputs.last_mut(), timers.iter().map(|timer| timer.due).max())
		{
			*slot = Some(TimedOutput::new(diffs, latest));
		}
		Ok(outputs)
	}

	fn seal_span(&self) -> Option<Duration> {
		SealRule::for_window(&self.kind, self.lateness().unwrap_or_else(Duration::zero))
			.map(|rule| rule.seal_span().duration())
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.core.output_schema.clone())
	}
}
