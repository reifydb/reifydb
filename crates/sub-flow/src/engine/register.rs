// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::{
		catalog::{
			flow::{FlowId, FlowNodeId},
			id::{RingBufferId, SeriesId, TableId, ViewId},
			series::SeriesKey,
			shape::ShapeId,
		},
		identifier::{ColumnIdentifier, ColumnShape},
	},
	internal,
	value::column::columns::Columns,
	window::engine::LatePolicy,
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::{
		flow::FlowDag,
		node::{
			FlowNode,
			FlowNodeType::{
				Aggregate, Append, Apply, Distinct, Extend, Filter, Gate, Join, Map,
				SinkRingBufferView, SinkSeriesView, SinkSubscription, SinkTableView, Sort,
				SourceDictionary, SourceFlow, SourceInlineData, SourceRingBuffer, SourceSeries,
				SourceTable, SourceView, Take, Window,
			},
		},
	},
};
use reifydb_sdk::config::Config;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	value::{dictionary::DictionaryId, duration::Duration},
};
use tracing::instrument;

use super::eval::evaluate_operator_config;
#[cfg(reifydb_target = "native")]
use crate::operator::apply::ApplyOperator;
use crate::{
	engine::FlowEngineInner,
	operator::{
		OperatorCell, Operators,
		append::AppendOperator,
		distinct::operator::DistinctOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		gate::GateOperator,
		join::operator::{JoinOperator, JoinSideConfig},
		map::MapOperator,
		scan::{
			dictionary::PrimitiveDictionaryOperator, flow::PrimitiveFlowOperator,
			ringbuffer::PrimitiveRingBufferOperator, series::PrimitiveSeriesOperator,
			table::PrimitiveTableOperator, view::PrimitiveViewOperator,
		},
		sink::{
			ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
			view::SinkTableViewOperator,
		},
		sort::SortOperator,
		take::TakeOperator,
		window::{
			aggregate::AggregateOperator,
			operator::{WindowConfig, WindowOperator},
		},
	},
};

impl FlowEngineInner {
	#[instrument(name = "flow::register", level = "info", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register(&mut self, txn: &mut CommandTransaction, flow: FlowDag) -> Result<()> {
		self.register_with_transaction(&mut Transaction::Command(txn), flow)
	}

	#[instrument(name = "flow::register_with_transaction", level = "info", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register_with_transaction(&mut self, txn: &mut Transaction<'_>, flow: FlowDag) -> Result<()> {
		reifydb_assertions! {
			assert!(!self.flows.contains_key(&flow.id), "Flow already registered");
		}

		let mut added: Vec<FlowNodeId> = Vec::new();
		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			if let Err(err) = self.add(txn, &flow, node) {
				for id in &added {
					self.operators.remove(id);
				}
				for entries in self.sources.values_mut() {
					entries.retain(|(fid, _)| *fid != flow.id);
				}
				self.sources.retain(|_, v| !v.is_empty());
				for entries in self.sinks.values_mut() {
					entries.retain(|(fid, _)| *fid != flow.id);
				}
				self.sinks.retain(|_, v| !v.is_empty());
				return Err(err);
			}
			added.push(node_id);
		}

		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow.clone());
		self.execution_level_cache.invalidate();
		self.schedule_cache.invalidate();

		Ok(())
	}

	#[instrument(name = "flow::add", level = "debug", skip(self, txn, flow), fields(flow_id = ?flow.id, node_id = ?node.id, node_type = ?mem::discriminant(&node.ty)))]
	pub fn add(&mut self, txn: &mut Transaction<'_>, flow: &FlowDag, node: &FlowNode) -> Result<()> {
		reifydb_assertions! {
			assert!(!self.operators.contains_key(&node.id), "Operator already registered");
		}
		let node = node.clone();
		let node_id = node.id;
		let inputs = node.inputs;

		match node.ty {
			SourceInlineData {
				..
			} => unimplemented!(),
			SourceTable {
				table,
			} => self.add_source_table(txn, flow, node_id, table)?,
			SourceView {
				view,
			} => self.register_source_view(txn, flow, node_id, view)?,
			SourceFlow {
				flow: source_flow,
			} => self.add_source_flow(txn, node_id, source_flow)?,
			SourceRingBuffer {
				ringbuffer,
			} => self.add_source_ringbuffer(txn, flow, node_id, ringbuffer)?,
			SourceSeries {
				series,
			} => self.add_source_series(txn, flow, node_id, series)?,
			SourceDictionary {
				dictionary,
			} => self.add_source_dictionary(flow, node_id, dictionary),
			SinkTableView {
				view,
				table,
			} => self.add_sink_table_view(txn, flow, node_id, &inputs, view, table)?,
			SinkRingBufferView {
				view,
				ringbuffer,
				capacity,
				propagate_evictions,
			} => self.add_sink_ringbuffer_view(
				txn,
				flow,
				node_id,
				&inputs,
				view,
				ringbuffer,
				capacity,
				propagate_evictions,
			)?,
			SinkSeriesView {
				view,
				series,
				key,
			} => self.add_sink_series_view(txn, flow, node_id, &inputs, view, series, key)?,
			SinkSubscription {
				..
			} => {
				return Err(Error(Box::new(internal!(
					"SinkSubscription nodes are no longer supported in persistent flows"
				))));
			}
			Filter {
				conditions,
			} => self.add_filter(node_id, &inputs, conditions)?,
			Gate {
				conditions,
			} => self.add_gate(node_id, &inputs, conditions)?,
			Map {
				expressions,
			} => self.add_map(node_id, &inputs, expressions)?,
			Extend {
				expressions,
			} => self.add_extend(node_id, &inputs, expressions)?,
			Sort {
				by: _,
			} => self.add_sort(node_id, &inputs)?,
			Take {
				limit,
			} => self.add_take(node_id, &inputs, limit)?,
			Join {
				join_type,
				left,
				right,
				alias,
				snapshot,
				natural,
				latest,
			} => self.add_join(
				txn, node_id, &inputs, join_type, left, right, alias, snapshot, natural, latest,
			)?,
			Distinct {
				expressions,
			} => self.add_distinct(txn, node_id, &inputs, expressions)?,
			Append {} => self.add_append(txn, node_id, &inputs)?,
			Apply {
				operator,
				expressions,
			} => self.add_apply(node_id, &inputs, operator, expressions)?,
			Aggregate {
				by,
				map,
			} => self.add_aggregate(node_id, &inputs, by, map)?,
			Window {
				kind,
				group_by,
				aggregations,
				ts,
				lateness,
			} => self.add_window(node_id, &inputs, kind, group_by, aggregations, ts, lateness)?,
		}

		Ok(())
	}

	#[inline]
	fn add_source_table(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		table: TableId,
	) -> Result<()> {
		let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

		self.add_source(flow.id, node_id, ShapeId::table(table.id));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceTable(PrimitiveTableOperator::new(node_id, table))),
		);
		Ok(())
	}

	#[inline]
	fn add_source_flow(
		&mut self,
		txn: &mut Transaction<'_>,
		node_id: FlowNodeId,
		source_flow: FlowId,
	) -> Result<()> {
		let source_flow = self.catalog.get_flow(&mut txn.reborrow(), source_flow)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceFlow(PrimitiveFlowOperator::new(node_id, source_flow))),
		);
		Ok(())
	}

	#[inline]
	fn add_source_ringbuffer(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		ringbuffer: RingBufferId,
	) -> Result<()> {
		let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
		self.add_source(flow.id, node_id, ShapeId::ringbuffer(rb.id));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceRingBuffer(PrimitiveRingBufferOperator::new(node_id, rb))),
		);
		Ok(())
	}

	#[inline]
	fn add_source_series(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		series: SeriesId,
	) -> Result<()> {
		let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
		self.add_source(flow.id, node_id, ShapeId::series(s.id));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceSeries(PrimitiveSeriesOperator::new(node_id))),
		);
		Ok(())
	}

	#[inline]
	fn add_source_dictionary(&mut self, flow: &FlowDag, node_id: FlowNodeId, dictionary: DictionaryId) {
		self.add_source(flow.id, node_id, ShapeId::dictionary(dictionary));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceDictionary(PrimitiveDictionaryOperator::new(node_id))),
		);
	}

	#[inline]
	fn add_sink_table_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		view: ViewId,
		table: TableId,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;

		self.add_sink(flow.id, node_id, ShapeId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkTableView(SinkTableViewOperator::new(
				parent, node_id, resolved, table,
			))),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_sink_ringbuffer_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		view: ViewId,
		ringbuffer: RingBufferId,
		capacity: u64,
		propagate_evictions: bool,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, node_id, ShapeId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkRingBufferView(SinkRingBufferViewOperator::new(
				parent,
				node_id,
				resolved,
				ringbuffer,
				capacity,
				propagate_evictions,
			))),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_sink_series_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		view: ViewId,
		series: SeriesId,
		key: SeriesKey,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, node_id, ShapeId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkSeriesView(SinkSeriesViewOperator::new(
				parent,
				node_id,
				resolved,
				series,
				key.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_filter(
		&mut self,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		conditions: Vec<Expression>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Filter(FilterOperator::new(
				parent,
				node_id,
				conditions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_gate(&mut self, node_id: FlowNodeId, inputs: &[FlowNodeId], conditions: Vec<Expression>) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Gate(GateOperator::new(
				parent,
				node_id,
				conditions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_map(&mut self, node_id: FlowNodeId, inputs: &[FlowNodeId], expressions: Vec<Expression>) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Map(MapOperator::new(
				parent,
				node_id,
				expressions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_extend(
		&mut self,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		expressions: Vec<Expression>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Extend(ExtendOperator::new(
				parent,
				node_id,
				expressions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_sort(&mut self, node_id: FlowNodeId, inputs: &[FlowNodeId]) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Sort(SortOperator::new(parent, node_id, Vec::new()))),
		);
		Ok(())
	}

	#[inline]
	fn add_take(&mut self, node_id: FlowNodeId, inputs: &[FlowNodeId], limit: usize) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators
			.insert(node_id, OperatorCell::new(Operators::Take(TakeOperator::new(parent, node_id, limit))));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_join(
		&mut self,
		txn: &mut Transaction<'_>,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		snapshot: bool,
		natural: bool,
		latest: bool,
	) -> Result<()> {
		if inputs.len() != 2 {
			return Err(Error(Box::new(internal!("Join node must have exactly 2 inputs"))));
		}

		let left_node = inputs[0];
		let right_node = inputs[1];

		let left_parent = self
			.operators
			.get(&left_node)
			.ok_or_else(|| Error(Box::new(internal!("Left parent operator not found"))))?
			.clone();

		let right_parent = self
			.operators
			.get(&right_node)
			.ok_or_else(|| Error(Box::new(internal!("Right parent operator not found"))))?
			.clone();

		let left_schema = left_parent.output_schema().unwrap_or_default();
		let right_schema =
			right_parent.output_schema().expect("right side of join must have a statically known schema");

		let (left_exprs, right_exprs) = if natural {
			let common = common_column_names(&left_schema, &right_schema);
			let keys: Vec<Expression> = common.iter().map(|name| natural_key_expr(name)).collect();
			(keys.clone(), keys)
		} else {
			(left, right)
		};

		let join_ttl = self.catalog.find_operator_settings(txn, node_id)?.and_then(|s| s.join);
		let left = join_ttl.as_ref().and_then(|j| j.left.as_ref());
		let left_ttl = left.map(|t| t.duration);
		let right = join_ttl.as_ref().and_then(|j| j.right.as_ref());
		let right_ttl = right.map(|t| t.duration);

		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Join(JoinOperator::new(
				JoinSideConfig {
					schema: left_schema,
					node: left_node,
					exprs: left_exprs,
				},
				JoinSideConfig {
					schema: right_schema,
					node: right_node,
					exprs: right_exprs,
				},
				node_id,
				join_type,
				alias,
				self.executor.clone(),
				snapshot,
				natural,
				latest,
				left_ttl,
				right_ttl,
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_distinct(
		&mut self,
		txn: &mut Transaction<'_>,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		expressions: Vec<Expression>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		let ttl = self.catalog.find_operator_settings(txn, node_id)?.and_then(|s| s.ttl);
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Distinct(DistinctOperator::new(
				parent,
				node_id,
				expressions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
				ttl.map(|t| {
					t.duration.as_nanos().expect("operator ttl duration fits in i64 nanoseconds")
						as u64
				}),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_append(&mut self, txn: &mut Transaction<'_>, node_id: FlowNodeId, inputs: &[FlowNodeId]) -> Result<()> {
		if inputs.len() < 2 {
			return Err(Error(Box::new(internal!("Append node must have at least 2 inputs"))));
		}

		let mut parents = Vec::with_capacity(inputs.len());

		for input_node_id in inputs {
			let parent = self
				.operators
				.get(input_node_id)
				.ok_or_else(|| {
					Error(Box::new(internal!(
						"Parent operator not found for input {:?}",
						input_node_id
					)))
				})?
				.clone();
			parents.push(parent);
		}

		let ttl = self.catalog.find_operator_settings(txn, node_id)?.and_then(|s| s.ttl);
		let ttl_nanos = ttl
			.as_ref()
			.map(|t| t.duration.as_nanos().expect("operator ttl duration fits in i64 nanoseconds") as u64);

		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Append(AppendOperator::new(
				node_id,
				parents,
				inputs.to_vec(),
				ttl_nanos,
				self.executor.runtime_context.version_epoch.clone(),
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_apply(
		&mut self,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		operator: String,
		expressions: Vec<Expression>,
	) -> Result<()> {
		let config = evaluate_operator_config(
			expressions.as_slice(),
			&self.executor.routines,
			&self.runtime_context,
		)?;
		let cfg = Config::new(operator.as_str(), config.clone());

		if let Some(factory) = self.custom_operators.get(operator.as_str()) {
			let op = factory(node_id, &cfg)?;
			self.operators.insert(node_id, OperatorCell::new(Operators::Custom(op)));
		} else {
			#[cfg(reifydb_target = "native")]
			{
				let parent = self.parent(first_input(inputs)?)?;

				let inner = if self.is_native_operator(operator.as_str()) {
					self.create_native_operator(operator.as_str(), node_id, &cfg)?
				} else if self.is_ffi_operator(operator.as_str()) {
					self.create_ffi_operator(operator.as_str(), node_id, &config)?
				} else {
					return Err(Error(Box::new(internal!("Unknown operator: {}", operator))));
				};

				self.operators.insert(
					node_id,
					OperatorCell::new(Operators::Apply(ApplyOperator::new(parent, node_id, inner))),
				);
			}
			#[cfg(not(reifydb_target = "native"))]
			{
				let _ = (operator, inputs);

				return Err(Error(Box::new(internal!("FFI operators are not supported in WASM"))));
			}
		}
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_window(
		&mut self,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		kind: WindowKind,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		ts: Option<String>,
		lateness: Option<Duration>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		let operator = WindowOperator::new(WindowConfig {
			parent,
			node: node_id,
			kind: kind.clone(),
			group_by: group_by.clone(),
			aggregations: aggregations.clone(),
			ts: ts.clone(),
			runtime_context: self.runtime_context.clone(),
			routines: self.executor.routines.clone(),
			late_policy: LatePolicy::Process,
			lateness,
		});
		self.operators.insert(node_id, OperatorCell::new(Operators::Window(operator)));
		Ok(())
	}

	#[inline]
	fn add_aggregate(
		&mut self,
		node_id: FlowNodeId,
		inputs: &[FlowNodeId],
		by: Vec<Expression>,
		map: Vec<Expression>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Aggregate(AggregateOperator::new(
				parent,
				node_id,
				by,
				map,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
			))),
		);
		Ok(())
	}

	fn parent(&self, input: FlowNodeId) -> Result<OperatorCell> {
		Ok(self.operators
			.get(&input)
			.ok_or_else(|| Error(Box::new(internal!("Parent operator not found"))))?
			.clone())
	}

	#[inline]
	fn register_source_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: FlowNodeId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, node_id, ShapeId::view(view.id()));

		self.add_source(flow.id, node_id, view.underlying_id());

		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceView(PrimitiveViewOperator::new(node_id, view))),
		);
		Ok(())
	}

	pub fn add_source(&mut self, flow: FlowId, node: FlowNodeId, shape: ShapeId) {
		let nodes = self.sources.entry(shape).or_default();

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	pub fn add_sink(&mut self, flow: FlowId, node: FlowNodeId, sink: ShapeId) {
		let nodes = self.sinks.entry(sink).or_default();

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}

fn first_input(inputs: &[FlowNodeId]) -> Result<FlowNodeId> {
	inputs.first().copied().ok_or_else(|| {
		Error(Box::new(internal!("flow node is missing a required input edge; the flow DAG is incomplete")))
	})
}

fn common_column_names(left: &Columns, right: &Columns) -> Vec<String> {
	let right_names: Vec<String> = right.names.iter().map(|n| n.text().to_string()).collect();
	left.names.iter().map(|n| n.text().to_string()).filter(|name| right_names.contains(name)).collect()
}

fn natural_key_expr(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		shape: ColumnShape::Qualified {
			namespace: Fragment::internal("_context"),
			name: Fragment::internal("_context"),
		},
		name: Fragment::internal(name),
	}))
}
