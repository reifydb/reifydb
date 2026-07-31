// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_engine::flow::aggregate::AggregateContext;
use reifydb_flow::{
	operator::{Operator, Reclaimable},
	transaction::FlowTransaction,
	window::{
		engine::{ExpiryAnchor, config::WindowEngineConfig, tumbling::TumblingBuckets},
		span::{WindowCoord, WindowSpan},
	},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};

use super::{
	accumulator::WindowSlotKey,
	core::Aggregation,
	engine::{finish_tumbling_engine, intern_window_groups, route_into_buckets},
};
use crate::{
	context::FlowContext,
	operator::{OperatorCell, store::OperatorStateStore},
};

type EngineBuckets = TumblingBuckets<Hash128, DateTime, (WindowSlotKey, Vec<Option<Value>>)>;

pub struct AggregateOperator {
	core: Aggregation,
	ttl: Option<Duration>,
}

impl AggregateOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		by: Vec<Expression>,
		map: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ttl: Option<Duration>,
	) -> Self {
		Self {
			core: Aggregation::new(
				node,
				parent,
				by,
				map,
				routines,
				runtime_context,
				AggregateContext::Grouped,
				Arc::new(FlowContext::default()),
			),
			ttl,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.core.parent.output_schema()
	}
}

impl Operator for AggregateOperator {
	fn id(&self) -> FlowNodeId {
		self.core.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_RECLAIM
	}

	fn retention_scale(&self) -> Option<Duration> {
		self.ttl
	}

	fn reclaimable_through(&self, _txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		Ok(self.ttl.map(|ttl| Reclaimable::data(watermark.saturating_sub(ttl))).unwrap_or_default())
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		apply_aggregate_engine(&self.core, txn, change)
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.core.tumbling_engine_invalidate(groups);
		self.core.engine_meta_invalidate(groups);
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.core.tumbling_engine_slot().as_ref().map(|engine| {
			OperatorSample::with_memory(engine.approximate_memory())
				.with_dirty_memory(engine.dirty_memory())
				.with_membership(engine.membership_memory())
				.with_completeness(engine.completeness())
		})
	}
}

pub fn apply_aggregate_engine(core: &Aggregation, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let budget = txn.state_budget();
	core.engine_meta_open(budget);
	let kinds = core.slot_kinds.clone().expect("aggregate requires representable slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

	let degenerate_span = |_row_idx: usize| {
		(
			WindowSpan::new(
				<DateTime as WindowCoord>::from_order(0),
				<DateTime as WindowCoord>::from_order(1),
			),
			DateTime::default(),
		)
	};

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_into_buckets(
				core,
				post,
				true,
				degenerate_span,
				&mut buckets,
				&mut group_values,
				&mut arrival,
				&mut window_max_ts,
			)?,
			Diff::Remove {
				pre,
				..
			} => route_into_buckets(
				core,
				pre,
				false,
				degenerate_span,
				&mut buckets,
				&mut group_values,
				&mut arrival,
				&mut window_max_ts,
			)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				route_into_buckets(
					core,
					pre,
					false,
					degenerate_span,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?;
				route_into_buckets(
					core,
					post,
					true,
					degenerate_span,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?;
			}
		}
	}

	let engine_config = WindowEngineConfig::builder(txn.state_budget()).build();

	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	let groups = intern_window_groups(core.node, txn, &windows)?;

	let diffs = finish_tumbling_engine(
		core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		Duration::default(),
		ExpiryAnchor::Unindexed,
	)?;
	core.engine_meta_flush(&mut OperatorStateStore::new(txn, core.node))?;
	Ok(Change::from_flow(core.node, change.version, diffs, change.changed_at))
}
