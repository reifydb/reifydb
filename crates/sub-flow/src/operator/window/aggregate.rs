// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	value::column::columns::Columns,
	window::{
		engine::{AccumulatorEvent, LatePolicy, tumbling::TumblingBuckets},
		span::WindowSpan,
	},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::{context::RuntimeContext, hash::Hash128};
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};

use super::{aggregation::Aggregation, tumbling::finish_tumbling_engine};
use crate::{
	operator::{Operator, OperatorCell},
	transaction::FlowTransaction,
};

#[inline]
pub(super) fn build_aggregation_shape(names: &[String], types: &[ValueType]) -> RowShape {
	let fields: Vec<RowShapeField> = names
		.iter()
		.zip(types.iter())
		.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
		.collect();
	RowShape::new(fields)
}

type EngineBuckets = TumblingBuckets<Hash128, u64, Vec<Option<Value>>>;

pub struct AggregateOperator {
	core: Aggregation,
}

impl AggregateOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		by: Vec<Expression>,
		map: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		Self {
			core: Aggregation::new(node, parent, by, map, routines, runtime_context),
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
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		apply_aggregate_engine(&self.core, txn, change)
	}
}

fn route_aggregate_columns(
	core: &Aggregation,
	columns: &Columns,
	is_add: bool,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
) -> Result<()> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = core.compute_groups(columns)?;
	let slot_cols = core.evaluate_slot_inputs(columns)?;
	let span = WindowSpan::new(0u64, 1u64);
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let contribution = core.build_contribution(columns, &slot_cols, row_idx);
		let key = (*hash, span);
		let event = if is_add {
			AccumulatorEvent::Add(contribution)
		} else {
			AccumulatorEvent::Remove(contribution)
		};
		if !buckets.contains_key(&key) {
			arrival.push(key);
		}
		buckets.entry(key).or_default().push(event);
		group_values.entry(*hash).or_insert_with(|| gvals.clone());
	}
	Ok(())
}

pub fn apply_aggregate_engine(core: &Aggregation, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = core.slot_kinds.clone().expect("aggregate requires representable slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_aggregate_columns(core, post, true, &mut buckets, &mut group_values, &mut arrival)?,
			Diff::Remove {
				pre,
				..
			} => route_aggregate_columns(core, pre, false, &mut buckets, &mut group_values, &mut arrival)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				route_aggregate_columns(
					core,
					pre,
					false,
					&mut buckets,
					&mut group_values,
					&mut arrival,
				)?;
				route_aggregate_columns(
					core,
					post,
					true,
					&mut buckets,
					&mut group_values,
					&mut arrival,
				)?;
			}
		}
	}

	let diffs = finish_tumbling_engine(
		core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		HashMap::new(),
		&kinds,
		LatePolicy::Process,
	)?;
	Ok(Change::from_flow(core.node, change.version, diffs, change.changed_at))
}
