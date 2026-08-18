// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::{
		catalog::{
			flow::{FlowId, OperatorId},
			id::{RingBufferId, SeriesId, TableId, ViewId},
			series::SeriesKey,
			object::ObjectId,
		},
		identifier::{ColumnIdentifier, ColumnObject},
	},
	value::column::columns::Columns,
};
use reifydb_rql::{
	expression::{ColumnExpression, Expression},
	flow::{
		flow::FlowDag,
		operator::{
			FlowNode,
			OperatorDef::{
				Aggregate, Append, Apply, Distinct, Extend, Filter, Gate, Join, Map,
				SinkRingBufferView, SinkSeriesView, SinkSubscription, SinkTableView, Sort,
				SourceInlineData, SourceRingBuffer, SourceSeries, SourceTable, SourceView, Take,
				Window,
			},
		},
	},
};
use reifydb_value::config::Config;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{Result, error::Error, fragment::Fragment, reifydb_assertions, value::duration::Duration};
use tracing::instrument;

use super::eval::evaluate_operator_config;
#[cfg(reifydb_target = "host")]
use crate::flow::operator::apply::ApplyOperator;
use crate::flow::{
	engine::FlowEngineInner,
	error::FlowGraphError,
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
			ringbuffer::PrimitiveRingBufferOperator, series::PrimitiveSeriesOperator,
			table::PrimitiveTableOperator, view::PrimitiveViewOperator,
		},
		sink::{
			ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
			view::SinkTableViewOperator,
		},
		sort::SortOperator,
		take::TakeOperator,
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

		let mut added: Vec<OperatorId> = Vec::new();
		for node_id in flow.topological_order()? {
			let node = flow.get_operator(&node_id).unwrap();
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
				table, ..
			} => self.add_source_table(txn, flow, node_id, table)?,
			SourceView {
				view,
			} => self.register_source_view(txn, flow, node_id, view)?,
			SourceRingBuffer {
				ringbuffer, ..
			} => self.add_source_ringbuffer(txn, flow, node_id, ringbuffer)?,
			SourceSeries {
				series, ..
			} => self.add_source_series(txn, flow, node_id, series)?,
			SinkTableView {
				view,
				table,
			} => self.add_sink_table_view(txn, flow, node_id, &inputs, view, table)?,
			SinkRingBufferView {
				view,
				ringbuffer,
				capacity,
			} => self.add_sink_ringbuffer_view(txn, flow, node_id, &inputs, view, ringbuffer, capacity)?,
			SinkSeriesView {
				view,
				series,
				key,
			} => self.add_sink_series_view(txn, flow, node_id, &inputs, view, series, key)?,
			SinkSubscription {
				..
			} => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "SinkSubscription",
				}));
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
			Aggregate { .. } | Window { .. } => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "aggregate/window",
				}));
			}
		}

		Ok(())
	}

	#[inline]
	fn add_source_table(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: OperatorId,
		table: TableId,
	) -> Result<()> {
		let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

		self.add_source(flow.id, node_id, ObjectId::table(table.id));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceTable(PrimitiveTableOperator::new(node_id, table))),
		);
		Ok(())
	}

	#[inline]
	fn add_source_ringbuffer(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: OperatorId,
		ringbuffer: RingBufferId,
	) -> Result<()> {
		let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
		self.add_source(flow.id, node_id, ObjectId::ringbuffer(rb.id));
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
		node_id: OperatorId,
		series: SeriesId,
	) -> Result<()> {
		let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
		self.add_source(flow.id, node_id, ObjectId::series(s.id));
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceSeries(PrimitiveSeriesOperator::new(node_id))),
		);
		Ok(())
	}

	#[inline]
	fn add_sink_table_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		table: TableId,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;

		self.add_sink(flow.id, node_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_table(&mut txn.reborrow(), table)?.partition_by;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkTableView(SinkTableViewOperator::new(
				parent,
				node_id,
				resolved,
				table,
				partition_by,
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
		node_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		ringbuffer: RingBufferId,
		capacity: u64,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, node_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?.partition_by;
		let propagate_evictions = true;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkRingBufferView(SinkRingBufferViewOperator::new(
				parent,
				node_id,
				resolved,
				ringbuffer,
				capacity,
				propagate_evictions,
				partition_by,
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
		node_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		series: SeriesId,
		key: SeriesKey,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, node_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_series(&mut txn.reborrow(), series)?.partition_by;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SinkSeriesView(SinkSeriesViewOperator::new(
				parent,
				node_id,
				resolved,
				series,
				key.clone(),
				partition_by,
			))),
		);
		Ok(())
	}

	#[inline]
	fn add_filter(
		&mut self,
		node_id: OperatorId,
		inputs: &[OperatorId],
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
	fn add_gate(&mut self, node_id: OperatorId, inputs: &[OperatorId], conditions: Vec<Expression>) -> Result<()> {
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
	fn add_map(&mut self, node_id: OperatorId, inputs: &[OperatorId], expressions: Vec<Expression>) -> Result<()> {
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
		node_id: OperatorId,
		inputs: &[OperatorId],
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
	fn add_sort(&mut self, node_id: OperatorId, inputs: &[OperatorId]) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::Sort(SortOperator::new(parent, node_id, Vec::new()))),
		);
		Ok(())
	}

	#[inline]
	fn add_take(&mut self, node_id: OperatorId, inputs: &[OperatorId], limit: usize) -> Result<()> {
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
		node_id: OperatorId,
		inputs: &[OperatorId],
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		snapshot: bool,
		natural: bool,
		latest: bool,
	) -> Result<()> {
		if inputs.len() != 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				node: "Join",
				expected: "exactly 2",
				found: inputs.len(),
			}));
		}

		let left_node = inputs[0];
		let right_node = inputs[1];

		let left_parent = self
			.operators
			.get(&left_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "left parent".to_string(),
				})
			})?
			.clone();

		let right_parent = self
			.operators
			.get(&right_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "right parent".to_string(),
				})
			})?
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
		node_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
	) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		let ttl = self.catalog.find_operator_settings(txn, node_id)?.and_then(|s| s.lateness);
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
	fn add_append(&mut self, txn: &mut Transaction<'_>, node_id: OperatorId, inputs: &[OperatorId]) -> Result<()> {
		if inputs.len() < 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				node: "Append",
				expected: "at least 2",
				found: inputs.len(),
			}));
		}

		let mut parents = Vec::with_capacity(inputs.len());

		for input_node_id in inputs {
			let parent = self
				.operators
				.get(input_node_id)
				.ok_or_else(|| {
					Error::from(FlowGraphError::ParentOperatorNotFound {
						input: format!("{:?}", input_node_id),
					})
				})?
				.clone();
			parents.push(parent);
		}

		let ttl = self.catalog.find_operator_settings(txn, node_id)?.and_then(|s| s.lateness);
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
		node_id: OperatorId,
		inputs: &[OperatorId],
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
			let _ = (operator, inputs);
			return Err(Error::from(FlowGraphError::FfiUnsupportedOnWasm));
		}
		Ok(())
	}

	fn parent(&self, input: OperatorId) -> Result<OperatorCell> {
		Ok(self.operators
			.get(&input)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: format!("{:?}", input),
				})
			})?
			.clone())
	}

	#[inline]
	fn register_source_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		node_id: OperatorId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, node_id, ObjectId::view(view.id()));

		self.add_source(flow.id, node_id, view.storage_id().into());

		self.operators.insert(
			node_id,
			OperatorCell::new(Operators::SourceView(PrimitiveViewOperator::new(node_id, view))),
		);
		Ok(())
	}

	pub fn add_source(&mut self, flow: FlowId, node: OperatorId, shape: ObjectId) {
		let nodes = self.sources.entry(shape).or_default();

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	pub fn add_sink(&mut self, flow: FlowId, node: OperatorId, sink: ObjectId) {
		let nodes = self.sinks.entry(sink).or_default();

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}

fn first_input(inputs: &[OperatorId]) -> Result<OperatorId> {
	inputs.first().copied().ok_or_else(|| Error::from(FlowGraphError::MissingInputEdge))
}

fn common_column_names(left: &Columns, right: &Columns) -> Vec<String> {
	let right_names: Vec<String> = right.names.iter().map(|n| n.text().to_string()).collect();
	left.names.iter().map(|n| n.text().to_string()).filter(|name| right_names.contains(name)).collect()
}

fn natural_key_expr(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Qualified {
			namespace: Fragment::internal("_context"),
			name: Fragment::internal("_context"),
		},
		name: Fragment::internal(name),
	}))
}
