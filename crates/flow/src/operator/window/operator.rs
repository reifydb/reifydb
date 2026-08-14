// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::{CommitVersion, WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	state::store::TimerKind,
	value::column::columns::Columns,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::{expression::Expression, flow::aggregate::AggregateContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result, reifydb_assertions,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration},
};

use super::{
	apply::{
		apply_session_engine, apply_sliding_engine, apply_tumbling_engine, reap_sealed_groups,
		seal_engine_windows, seal_session_engine,
	},
	rolling::{apply_rolling_engine, seal_rolling_engine},
};
use crate::{
	context::FlowContext,
	operator::{
		HostOperator,
		aggregation::{accumulator::RowAccumulator, core::Aggregation},
		drops::SealedDrops,
		host::HostContext,
	},
	state::seal::ledger::FiredAt,
	timer::Timer,
	window::{
		coord::OrdinalCoord,
		engine::{config::WindowEngineConfig, rolling::RollingEngine},
		meta::WindowMeta,
	},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

pub struct WindowConfig {
	pub parent_schema: Option<Columns>,
	pub operator: OperatorId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub seal: Duration,
	pub ctx: Arc<FlowContext>,
}

pub(crate) enum RollingEngineSlot {
	CountedRow(Box<RollingEngine<Hash128, OrdinalCoord, RowAccumulator>>),
	TimedRow(Box<RollingEngine<Hash128, DateTime, RowAccumulator>>),
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,

	pub seal: Duration,
	sealed_drops: SealedDrops,
	rolling_engine: Option<RollingEngineSlot>,
	meta: WindowMeta,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Self {
		let core = Aggregation::new(
			config.operator,
			config.parent_schema,
			config.group_by,
			config.aggregations,
			config.routines,
			config.runtime_context,
			AggregateContext::Windowed,
			config.ctx,
		);
		Self {
			core,
			kind: config.kind,
			seal: config.seal,
			sealed_drops: SealedDrops::new(config.operator, "mutations targeting sealed windows"),
			rolling_engine: None,
			meta: WindowMeta::new(),
		}
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

	pub fn seal(&self) -> Duration {
		if self.is_count_based() {
			Duration::default()
		} else {
			self.seal
		}
	}

	pub fn seal_ms(&self) -> u64 {
		self.seal().milliseconds().unwrap_or(0) as u64
	}

	pub(crate) fn note_sealed_drops(&self, dropped: u64) {
		self.sealed_drops.note(dropped);
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

	pub fn row_times(&self, columns: &Columns, row_count: usize) -> Result<Vec<DateTime>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		reifydb_assertions! {
			assert!(
				columns.time().len() >= row_count,
				"a window buckets by #time, which the substrate populates on every row before any \
				 operator sees it, in both time domains; a short #time vector means a producer \
				 skipped stamping and the window would silently bucket by wall clock \
				 (time={} rows={row_count})",
				columns.time().len()
			);
		}
		Ok((0..row_count).map(|i| columns.time().get(i).copied().unwrap_or_default()).collect())
	}
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
		if timer.kind == TimerKind::Maintenance {
			reap_sealed_groups(self, host)?;
			return Ok(None);
		}
		let fired = FiredAt::of(&timer);
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
			} => seal_session_engine(self, host, fired)?,
			_ => vec![],
		};

		if diffs.is_empty() {
			Ok(None)
		} else {
			Ok(Some(Change::from_flow(self.core.operator, CommitVersion(0), diffs, timer.due)))
		}
	}

	fn output_schema(&self) -> Option<Columns> {
		self.core.parent_schema.clone()
	}
}
