// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	common::{CommitVersion, WindowKind, WindowSize},
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
	state::{budget::OperatorStateBudgetHandle, horizon::window_horizon},
	value::column::columns::Columns,
};
use reifydb_engine::flow::aggregate::AggregateContext;
use reifydb_flow::{
	operator::Operator,
	timer::Timer,
	transaction::FlowTransaction,
	window::{
		aux::WindowAux,
		engine::{config::WindowEngineConfig, rolling::RollingEngine},
		ledger::FiredAt,
		span::WindowCoord,
	},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result, reifydb_assertions,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration},
};

use super::{
	accumulator::RowAccumulator,
	aggregation::Aggregation,
	rolling::{apply_rolling_engine, seal_rolling_engine},
	tumbling::{
		apply_session_engine, apply_sliding_engine, apply_tumbling_engine, seal_engine_windows,
		seal_session_engine,
	},
};
use crate::{
	context::FlowContext,
	operator::{
		OperatorCell,
		drops::SealedDrops,
		stateful::{raw::RawStatefulOperator, window::WindowStateful},
		store::OperatorStateStore,
	},
};

const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

pub struct WindowConfig {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub grace: Duration,
	pub lateness: Duration,
	pub state_budget: OperatorStateBudgetHandle,
	pub ctx: Arc<FlowContext>,
}

pub(crate) enum RollingEngineSlot {
	CountedRow(Box<RollingEngine<Hash128, u64, RowAccumulator>>),
	TimedRow(Box<RollingEngine<Hash128, DateTime, RowAccumulator>>),
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,

	pub grace: Duration,
	pub lateness: Duration,
	pub state_budget: OperatorStateBudgetHandle,
	pub layout: RowShape,
	sealed_drops: SealedDrops,
	rolling_engine: UnsafeCell<Option<RollingEngineSlot>>,
	aux: UnsafeCell<WindowAux>,
}

impl WindowOperator {
	pub fn new(config: WindowConfig) -> Self {
		let core = Aggregation::new(
			config.node,
			config.parent,
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
			grace: config.grace,
			lateness: config.lateness,
			state_budget: config.state_budget.clone(),
			layout: RowShape::operator_state(),
			sealed_drops: SealedDrops::new(config.node, "mutations targeting sealed windows"),
			rolling_engine: UnsafeCell::new(None),
			aux: UnsafeCell::new(WindowAux::new(config.state_budget)),
		}
	}

	#[allow(clippy::mut_from_ref)]
	pub(super) fn aux_slot(&self) -> &mut WindowAux {
		// SAFETY: apply and tick run single-threaded and never re-enter, so aux_slot borrows are

		unsafe { &mut *self.aux.get() }
	}

	fn with_aux<R>(
		&self,
		txn: &mut FlowTransaction,
		f: impl FnOnce(&mut FlowTransaction) -> Result<R>,
	) -> Result<R> {
		let node = self.core.node;
		let budget = txn.state_budget();
		self.aux_slot().hydrate_once(&mut OperatorStateStore::new(txn, node))?;
		self.core.engine_meta_open(budget);
		let out = f(txn)?;
		self.aux_slot().flush(&mut OperatorStateStore::new(txn, node))?;
		self.core.engine_meta_flush(&mut OperatorStateStore::new(txn, node))?;
		Ok(out)
	}

	#[allow(clippy::mut_from_ref)]
	pub(crate) fn rolling_engine_slot(&self) -> &mut Option<RollingEngineSlot> {
		unsafe { &mut *self.rolling_engine.get() }
	}

	pub(crate) fn engine_config(&self) -> WindowEngineConfig {
		WindowEngineConfig::builder(self.state_budget.clone()).build()
	}

	pub fn is_count_based(&self) -> bool {
		self.kind.size().is_some_and(|m| m.is_count())
	}

	pub fn grace(&self) -> Duration {
		if self.is_count_based() {
			Duration::default()
		} else {
			self.grace
		}
	}

	pub fn grace_ms(&self) -> u64 {
		self.grace().milliseconds().unwrap_or(0) as u64
	}

	pub fn lateness(&self) -> Duration {
		if self.is_count_based() {
			Duration::default()
		} else {
			self.lateness
		}
	}

	pub fn lateness_ms(&self) -> u64 {
		self.lateness().milliseconds().unwrap_or(0) as u64
	}

	pub(crate) fn note_sealed_drops(&self, dropped: u64) {
		self.sealed_drops.note(dropped);
	}

	pub fn is_rolling(&self) -> bool {
		matches!(self.kind, WindowKind::Rolling { .. })
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
		Ok((0..row_count)
			.map(|i| {
				columns.time().get(i).map_or(DateTime::default(), |dt| {
					<DateTime as WindowCoord>::from_order(dt.timestamp_millis() as u64)
				})
			})
			.collect())
	}
}

impl RawStatefulOperator for WindowOperator {}

impl WindowStateful for WindowOperator {
	fn layout(&self) -> RowShape {
		self.layout.clone()
	}
}

impl Operator for WindowOperator {
	fn id(&self) -> FlowNodeId {
		self.core.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn seal_after_ms(&self) -> Option<u64> {
		window_horizon(&self.kind, self.grace(), self.lateness()).span_ms()
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.core.tumbling_engine_invalidate(groups);
		self.core.engine_meta_invalidate(groups);
		if let Some(slot) = self.rolling_engine_slot().as_mut() {
			match slot {
				RollingEngineSlot::CountedRow(engine) => engine.invalidate_groups(groups),
				RollingEngineSlot::TimedRow(engine) => engine.invalidate_groups(groups),
			};
		}
		self.aux_slot().invalidate_groups(groups);
	}

	fn sample(&self) -> Option<OperatorSample> {
		let (mut memory, mut dirty, mut membership, mut completeness) =
			if let Some(slot) = self.rolling_engine_slot().as_ref() {
				match slot {
					RollingEngineSlot::CountedRow(engine) => (
						engine.approximate_memory(),
						engine.dirty_memory(),
						engine.membership_memory(),
						engine.completeness(),
					),
					RollingEngineSlot::TimedRow(engine) => (
						engine.approximate_memory(),
						engine.dirty_memory(),
						engine.membership_memory(),
						engine.completeness(),
					),
				}
			} else {
				let engine = self.core.tumbling_engine_slot().as_ref()?;
				(
					engine.approximate_memory(),
					engine.dirty_memory(),
					engine.membership_memory(),
					engine.completeness(),
				)
			};

		let (aux_memory, aux_dirty, aux_membership, aux_completeness) = self.aux_slot().sample_parts();
		memory = memory + aux_memory;
		dirty = dirty + aux_dirty;
		membership = membership + aux_membership;
		completeness = completeness.merge(aux_completeness);

		if let Some((em_memory, em_dirty, em_membership, em_completeness)) =
			self.core.engine_meta_sample_parts()
		{
			memory = memory + em_memory;
			dirty = dirty + em_dirty;
			membership = membership + em_membership;
			completeness = completeness.merge(em_completeness);
		}

		Some(OperatorSample::with_memory(memory)
			.with_dirty_memory(dirty)
			.with_membership(membership)
			.with_completeness(completeness))
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.with_aux(txn, |txn| match &self.kind {
			WindowKind::Tumbling {
				..
			} => apply_tumbling_engine(self, txn, change),
			WindowKind::Sliding {
				..
			} => apply_sliding_engine(self, txn, change),
			WindowKind::Rolling {
				..
			} => apply_rolling_engine(self, txn, change),
			WindowKind::Session {
				..
			} => apply_session_engine(self, txn, change),
		})
	}

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		let fired = FiredAt::of(&timer);
		self.with_aux(txn, |txn| {
			let diffs = match &self.kind {
				WindowKind::Tumbling {
					..
				}
				| WindowKind::Sliding {
					..
				} => seal_engine_windows(self, txn, fired)?,
				WindowKind::Rolling {
					size: WindowSize::Duration(_),
					..
				} => seal_rolling_engine(self, txn, fired)?,
				WindowKind::Session {
					..
				} => seal_session_engine(self, txn, fired)?,
				_ => vec![],
			};

			if diffs.is_empty() {
				Ok(None)
			} else {
				Ok(Some(Change::from_flow(self.core.node, CommitVersion(0), diffs, timer.at)))
			}
		})
	}
}
