// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::{expression::Expression, flow::aggregate::AggregateContext};
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
	operator::{HostOperator, host::HostContext, state::seal::coord::Coord},
	window::{
		engine::{ExpiryAnchor, config::WindowEngineConfig, tumbling::TumblingBuckets},
		span::WindowSpan,
	},
};

type EngineBuckets = TumblingBuckets<Hash128, DateTime, (WindowSlotKey, Vec<Option<Value>>)>;

pub struct AggregateOperator {
	core: Aggregation,
	_lateness: Option<Duration>,
}

impl AggregateOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		by: Vec<Expression>,
		map: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		lateness: Option<Duration>,
	) -> Self {
		Self {
			core: Aggregation::new(
				operator,
				parent_schema,
				by,
				map,
				routines,
				runtime_context,
				AggregateContext::Grouped,
				Arc::new(FlowContext::default()),
			),
			_lateness: lateness,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.core.parent_schema.clone()
	}
}

impl HostOperator for AggregateOperator {
	fn id(&self) -> OperatorId {
		self.core.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		apply_aggregate_engine(&mut self.core, host, change)
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

pub fn apply_aggregate_engine(core: &mut Aggregation, host: &mut dyn HostContext, change: Change) -> Result<Change> {
	let kinds = core.slot_kinds.clone().expect("aggregate requires representable slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

	let degenerate_span = |_row_idx: usize| {
		(
			WindowSpan::new(<DateTime as Coord>::from_order(0), <DateTime as Coord>::from_order(1)),
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

	let engine_config = WindowEngineConfig::builder().build();

	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	let groups = intern_window_groups(host, &windows)?;

	let diffs = finish_tumbling_engine(
		core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		None,
		ExpiryAnchor::Unindexed,
	)?;
	Ok(Change::from_flow(core.operator, change.version, diffs, change.changed_at))
}
