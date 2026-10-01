// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, WindowKind, WindowSize},
	expression::Expression,
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	state::timer::TimerKind,
};
use reifydb_flow::{aggregate::AggregateContext, context::FlowContext};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{
		datetime::DateTime,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{require_row_numbers, require_time},
	},
};

use super::{
	apply::{
		apply_session_engine, apply_sliding_engine, apply_tumbling_engine, reap_sealed_groups,
		seal_engine_windows, seal_session_engine,
	},
	rolling::{apply_rolling_engine, seal_rolling_engine},
};
use crate::{
	operator::{
		HostOperator,
		aggregation::{accumulator::RowAccumulator, core::Aggregation},
		drops::SealedDrops,
		host::HostContext,
		state::seal::{ledger::FiredAt, rule::SealRule},
	},
	timer::Timer,
	window::{
		coord::OrdinalCoord,
		engine::{config::WindowEngineConfig, rolling::RollingEngine},
		meta::WindowMeta,
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

pub(crate) enum RollingEngineSlot {
	CountedRow(Box<RollingEngine<Hash128, OrdinalCoord, RowAccumulator>>),
	TimedRow(Box<RollingEngine<Hash128, DateTime, RowAccumulator>>),
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,

	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
	sealed_drops: SealedDrops,
	refused_rows: SealedDrops,
	rolling_engine: Option<RollingEngineSlot>,
	meta: WindowMeta,
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
		Ok(Self {
			core,
			kind: config.kind,
			lateness: config.lateness,
			immutable: config.immutable,
			sealed_drops: SealedDrops::new(config.operator, "mutations targeting sealed windows"),
			refused_rows: SealedDrops::new(config.operator, "session rows refused by assignment"),
			rolling_engine: None,
			meta: WindowMeta::new(),
		})
	}

	pub(super) fn meta_slot(&mut self) -> &mut WindowMeta {
		&mut self.meta
	}

	pub(crate) fn rolling_engine_slot(&mut self) -> &mut Option<RollingEngineSlot> {
		&mut self.rolling_engine
	}

	pub(crate) fn engine_config(&self) -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	pub fn is_count_based(&self) -> bool {
		self.kind.size().is_some_and(|m| m.is_count())
	}

	pub fn lateness(&self) -> Option<Duration> {
		if self.is_count_based() {
			None
		} else {
			self.lateness
		}
	}

	pub fn immutable(&self) -> Option<Duration> {
		if self.is_count_based() {
			None
		} else {
			self.immutable
		}
	}

	pub(crate) fn note_sealed_drops(&self, dropped: u64) {
		self.sealed_drops.note(dropped);
	}

	pub(crate) fn note_refused_rows(&self, refused: u64) {
		self.refused_rows.note(refused);
	}

	pub fn size_duration(&self) -> Option<Duration> {
		self.kind.size().and_then(|m| m.as_duration())
	}

	pub fn size_count(&self) -> Option<u64> {
		self.kind.size().and_then(|m| m.as_count())
	}

	pub fn rolling_lag(&self) -> Duration {
		if self.is_count_based() {
			return Duration::default();
		}
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
		let out = match self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_engine(self, host, change),
			WindowKind::Sliding {
				..
			} => apply_sliding_engine(self, host, change),
			WindowKind::Rolling {
				..
			} => apply_rolling_engine(self, host, change),
			WindowKind::Session {
				..
			} => apply_session_engine(self, host, change),
		}?;
		Ok(out)
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		let fired = FiredAt::of(&timer);
		if timer.kind == TimerKind::Maintenance {
			reap_sealed_groups(self, host, fired)?;
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
			} => seal_rolling_engine(self, host, fired)?,
			WindowKind::Session {
				..
			} => seal_session_engine(self, host, fired, &timer.key)?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Change::from_flow(
				self.core.operator,
				ChangeVersion::from(CommitVersion(0)),
				diffs,
				timer.due,
			)))
		}
	}

	fn seal_span(&self) -> Option<Duration> {
		SealRule::for_window(&self.kind, self.lateness().unwrap_or_else(Duration::zero))
			.map(|rule| rule.admissible().duration())
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.core.output_schema.clone())
	}
}
