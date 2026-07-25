// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::UnsafeCell,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	common::{CommitVersion, TimeDomain, WindowKind, WindowSize},
	error::diagnostic::flow::{flow_window_timestamp_column_not_found, flow_window_timestamp_column_type_mismatch},
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
	state::budget::OperatorStateBudgetHandle,
	value::column::columns::Columns,
	window::engine::{config::WindowEngineConfig, rolling::RollingEngine},
};
use reifydb_engine::flow::aggregate::AggregateContext;
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{Value, duration::Duration},
};
use tracing::warn;

use super::{
	accumulator::{RowAccumulator, StampedAccumulator},
	aggregation::Aggregation,
	aux::WindowAux,
	rolling::{
		apply_rolling_engine, apply_rolling_processing_engine, tick_expire_rolling_engine,
		tick_expire_rolling_processing_engine,
	},
	tumbling::{
		apply_session_engine, apply_sliding_engine, apply_tumbling_engine, tick_expire_engine_windows,
		tick_expire_session_engine,
	},
};
use crate::{
	context::FlowContext,
	operator::{
		OperatorCell,
		stateful::{raw::RawStatefulOperator, window::WindowStateful},
		store::OperatorStateStore,
	},
};

/// Tick stays: it is how sealed windows are emitted, which is engine semantics
/// rather than retention. Reclaim is what lets the substrate erase a window group
/// once its seal horizon has passed.
const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Tick,
	OperatorCapability::Reclaim,
];

pub struct WindowConfig {
	pub parent: OperatorCell,
	pub node: FlowNodeId,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub runtime_context: RuntimeContext,
	pub routines: Routines,
	pub grace: Duration,
	pub lateness: Duration,
	pub state_budget: OperatorStateBudgetHandle,
	pub ctx: Arc<FlowContext>,
}

pub(crate) enum RollingEngineSlot {
	Row(Box<RollingEngine<Hash128, u64, RowAccumulator>>),
	Stamped(Box<RollingEngine<Hash128, u64, StampedAccumulator>>),
}

pub struct WindowOperator {
	pub core: Aggregation,
	pub kind: WindowKind,
	pub ts: Option<String>,

	pub grace: Duration,
	pub lateness: Duration,
	pub state_budget: OperatorStateBudgetHandle,
	pub layout: RowShape,
	last_rolling_expiry_ms: AtomicU64,
	sealed_drops: AtomicU64,
	rolling_engine: UnsafeCell<Option<RollingEngineSlot>>,
	aux: UnsafeCell<WindowAux>,
}

impl WindowOperator {
	fn rolling_expiry_due(&self, now_ms: u64) -> bool {
		let size_ms = self.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);
		let stride_ms = (size_ms / 240).clamp(1_000, 30_000);
		let last = self.last_rolling_expiry_ms.load(Ordering::Relaxed);
		if now_ms.saturating_sub(last) < stride_ms {
			return false;
		}
		self.last_rolling_expiry_ms.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok()
	}

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
			ts: config.ts,
			grace: config.grace,
			lateness: config.lateness,
			state_budget: config.state_budget.clone(),
			layout: RowShape::operator_state(),
			last_rolling_expiry_ms: AtomicU64::new(0),
			sealed_drops: AtomicU64::new(0),
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
		if dropped == 0 {
			return;
		}
		let before = self.sealed_drops.fetch_add(dropped, Ordering::Relaxed);
		let after = before + dropped;
		if before == 0 || before / 1_000 != after / 1_000 {
			warn!(
				node_id = self.core.node.0,
				dropped,
				total = after,
				"mutations targeting sealed windows were dropped"
			);
		}
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

	pub fn resolve_event_timestamps(&self, columns: &Columns, row_count: usize) -> Result<Vec<u64>> {
		if row_count == 0 {
			return Ok(Vec::new());
		}
		match (self.kind.time(), &self.ts) {
			(TimeDomain::Event, Some(ts_col)) => {
				let col = columns.column(ts_col).ok_or_else(|| {
					Error(Box::new(flow_window_timestamp_column_not_found(ts_col)))
				})?;
				let mut timestamps = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match col.data().get_value(i) {
						Value::DateTime(dt) => timestamps.push(dt.timestamp_millis() as u64),
						other => {
							return Err(Error(Box::new(
								flow_window_timestamp_column_type_mismatch(
									ts_col,
									other.get_type(),
								),
							)));
						}
					}
				}
				Ok(timestamps)
			}
			_ => {
				let now = self.core.current_timestamp();
				Ok(vec![now; row_count])
			}
		}
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

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.core.tumbling_engine_invalidate(groups);
		self.core.engine_meta_invalidate(groups);
	}

	fn sample(&self) -> Option<OperatorSample> {
		let (mut memory, mut dirty, mut membership, mut completeness) =
			if let Some(slot) = self.rolling_engine_slot().as_ref() {
				match slot {
					RollingEngineSlot::Row(engine) => (
						engine.approximate_memory(),
						engine.dirty_memory(),
						engine.membership_memory(),
						engine.completeness(),
					),
					RollingEngineSlot::Stamped(engine) => (
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

	fn ticks(&self) -> Option<Duration> {
		match &self.kind {
			WindowKind::Tumbling {
				..
			}
			| WindowKind::Sliding {
				..
			}
			| WindowKind::Session {
				..
			}
			| WindowKind::Rolling {
				size: WindowSize::Duration(_),
				..
			} => Some(Duration::from_seconds(1).unwrap()),
			WindowKind::Rolling {
				size: WindowSize::Count(_),
				..
			} => None,
		}
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
			} => {
				if self.is_rolling_processing() {
					apply_rolling_processing_engine(self, txn, change)
				} else {
					apply_rolling_engine(self, txn, change)
				}
			}
			WindowKind::Session {
				..
			} => apply_session_engine(self, txn, change),
		})
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let current_timestamp = tick.now.to_nanos() / 1_000_000;
		self.with_aux(txn, |txn| {
			let diffs = match &self.kind {
				WindowKind::Tumbling {
					..
				}
				| WindowKind::Sliding {
					..
				} => tick_expire_engine_windows(self, txn, current_timestamp)?,
				WindowKind::Rolling {
					size: WindowSize::Duration(_),
					..
				} if !self.rolling_expiry_due(current_timestamp) => vec![],
				WindowKind::Rolling {
					size: WindowSize::Duration(_),
					..
				} if self.is_rolling_processing() => tick_expire_rolling_processing_engine(self, txn, current_timestamp)?,
				WindowKind::Rolling {
					size: WindowSize::Duration(_),
					..
				} => tick_expire_rolling_engine(self, txn, current_timestamp)?,
				WindowKind::Session {
					..
				} => tick_expire_session_engine(self, txn, current_timestamp)?,
				_ => vec![],
			};

			if diffs.is_empty() {
				Ok(None)
			} else {
				Ok(Some(Change::from_flow(
					self.core.node,
					CommitVersion(0),
					diffs,
					self.core.runtime_context.clock.now(),
				)))
			}
		})
	}
}
