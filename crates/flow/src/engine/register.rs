// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::{
		catalog::{
			flow::{FlowId, OperatorId},
			id::{RingBufferId, SeriesId, TableId, ViewId},
			object::ObjectId,
			series::SeriesKey,
			storage::StorageId,
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
		time_domain::check_window_time_requirements,
	},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	Result, config::Config, error::Error, fragment::Fragment, reifydb_assertions, value::duration::Duration,
};
use tracing::{info, instrument};

use super::eval::evaluate_operator_config;
use crate::{
	context::FlowContext,
	engine::FlowEngineInner,
	error::FlowGraphError,
	operator::{
		BoxedOperator,
		aggregation::operator::AggregateOperator,
		append::AppendOperator,
		apply::ApplyOperator,
		distinct::operator::DistinctOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		gate::GateOperator,
		join::operator::{JoinOperator, JoinSideConfig},
		map::MapOperator,
		scan::{
			ringbuffer::SourceRingBufferOperator, series::SourceSeriesOperator, table::SourceTableOperator,
			view::SourceViewOperator,
		},
		sink::{
			ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
			view::SinkTableViewOperator,
		},
		sort::SortOperator,
		take::TakeOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
	transaction::{FlowTransaction, deferred::DeferredTransaction},
};

impl FlowEngineInner<DeferredTransaction> {
	#[instrument(name = "flow::register", level = "info", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register(&mut self, txn: &mut CommandTransaction, flow: FlowDag) -> Result<()> {
		self.register_with_transaction(&mut Transaction::Command(txn), flow)
	}

	#[instrument(name = "flow::register_with_transaction", level = "info", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register_with_transaction(&mut self, txn: &mut Transaction<'_>, flow: FlowDag) -> Result<()> {
		reifydb_assertions! {
			assert!(!self.flows.contains_key(&flow.id), "Flow already registered");
		}

		check_window_time_requirements(&self.catalog, txn, &flow)?;

		if !flow.has_timed_source() {
			info!(
				flow_id = flow.id.0,
				"no temporal sources; no timers will fire and no window can seal in this flow, so its \
				 rows propagate but never age"
			);
		}

		let mut added: Vec<OperatorId> = Vec::new();
		let ctx = Arc::new(FlowContext::default());
		for operator_id in flow.topological_order()? {
			let operator = flow.get_operator(&operator_id).unwrap();
			if let Err(err) = self.add(txn, &flow, operator, &ctx) {
				for id in &added {
					self.operators.remove(id);
					self.durable_sinks.remove(id);
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
			added.push(operator_id);
		}

		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow.clone());

		Ok(())
	}

	#[instrument(name = "flow::add", level = "debug", skip(self, txn, flow, ctx), fields(flow_id = ?flow.id, operator_id = ?operator.id, node_type = ?mem::discriminant(&operator.ty)))]
	pub fn add(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator: &FlowNode,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let operator_id = operator.id;
		let inputs = operator.inputs.clone();

		match operator.ty.clone() {
			SinkTableView {
				view,
				table,
			} => {
				reifydb_assertions! {
					assert!(!self.durable_sinks.contains_key(&operator_id), "Operator already registered");
				}
				self.add_sink_table_view(txn, flow, operator_id, &inputs, view, table)
			}
			SinkRingBufferView {
				view,
				ringbuffer,
				capacity,
			} => {
				reifydb_assertions! {
					assert!(!self.durable_sinks.contains_key(&operator_id), "Operator already registered");
				}
				self.add_sink_ringbuffer_view(
					txn,
					flow,
					operator_id,
					&inputs,
					view,
					ringbuffer,
					capacity,
				)
			}
			SinkSeriesView {
				view,
				series,
				key,
			} => {
				reifydb_assertions! {
					assert!(!self.durable_sinks.contains_key(&operator_id), "Operator already registered");
				}
				self.add_sink_series_view(txn, flow, operator_id, &inputs, view, series, key)
			}
			_ => self.add_core(txn, flow, operator, ctx),
		}
	}

	#[inline]
	fn add_sink_table_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		table: TableId,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_table(&mut txn.reborrow(), table)?.partition_by;
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkTableViewOperator::new(
				operator_id,
				resolved,
				table,
				partition_by,
			)),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_sink_ringbuffer_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		ringbuffer: RingBufferId,
		capacity: u64,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?.partition_by;
		let ttl = self
			.catalog
			.find_row_settings(&mut txn.reborrow(), StorageId::ringbuffer(ringbuffer))?
			.and_then(|settings| settings.ttl);
		let announce_evictions = ttl.as_ref().map(|ttl| ttl.announce).unwrap_or(true);
		let row_ttl = ttl.as_ref().map(|t| t.duration);
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkRingBufferViewOperator::new(
				operator_id,
				resolved,
				ringbuffer,
				capacity,
				announce_evictions,
				row_ttl,
				partition_by,
			)),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_sink_series_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		view: ViewId,
		series: SeriesId,
		key: SeriesKey,
	) -> Result<()> {
		self.require_parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_series(&mut txn.reborrow(), series)?.partition_by;
		self.durable_sinks.insert(
			operator_id,
			Box::new(SinkSeriesViewOperator::new(
				operator_id,
				resolved,
				series,
				key.clone(),
				partition_by,
			)),
		);
		Ok(())
	}
}

impl<T: FlowTransaction> FlowEngineInner<T> {
	#[instrument(name = "flow::add_core", level = "debug", skip(self, txn, flow, ctx), fields(flow_id = ?flow.id, operator_id = ?operator.id, node_type = ?mem::discriminant(&operator.ty)))]
	pub fn add_core(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator: &FlowNode,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		reifydb_assertions! {
			assert!(!self.operators.contains_key(&operator.id), "Operator already registered");
		}
		let operator = operator.clone();
		let operator_id = operator.id;
		let inputs = operator.inputs;

		match operator.ty {
			SourceInlineData {
				..
			} => unimplemented!(),
			SourceTable {
				table,
				..
			} => self.add_source_table(txn, flow, operator_id, table)?,
			SourceView {
				view,
			} => self.register_source_view(txn, flow, operator_id, view)?,
			SourceRingBuffer {
				ringbuffer,
				..
			} => self.add_source_ringbuffer(txn, flow, operator_id, ringbuffer)?,
			SourceSeries {
				series,
				..
			} => self.add_source_series(txn, flow, operator_id, series)?,
			SinkTableView {
				..
			} => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "SinkTableView",
				}));
			}
			SinkRingBufferView {
				..
			} => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "SinkRingBufferView",
				}));
			}
			SinkSeriesView {
				..
			} => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "SinkSeriesView",
				}));
			}
			SinkSubscription {
				..
			} => {
				return Err(Error::from(FlowGraphError::UnsupportedNode {
					kind: "SinkSubscription",
				}));
			}
			Filter {
				conditions,
			} => self.add_filter(operator_id, &inputs, conditions, ctx)?,
			Gate {
				conditions,
			} => self.add_gate(operator_id, &inputs, conditions, ctx)?,
			Map {
				expressions,
			} => self.add_map(operator_id, &inputs, expressions, ctx)?,
			Extend {
				expressions,
			} => self.add_extend(operator_id, &inputs, expressions, ctx)?,
			Sort {
				by: _,
			} => self.add_sort(operator_id, &inputs)?,
			Take {
				limit,
			} => self.add_take(operator_id, &inputs, limit)?,
			Join {
				join_type,
				left,
				right,
				alias,
				snapshot,
				natural,
				latest,
			} => self.add_join(
				txn,
				operator_id,
				&inputs,
				join_type,
				left,
				right,
				alias,
				snapshot,
				natural,
				latest,
				ctx,
			)?,
			Distinct {
				expressions,
			} => self.add_distinct(txn, operator_id, &inputs, expressions, ctx)?,
			Append {} => self.add_append(txn, operator_id, &inputs)?,
			Apply {
				operator,
				expressions,
			} => self.add_apply(txn, operator_id, &inputs, operator, expressions)?,
			Aggregate {
				by,
				map,
			} => self.add_aggregate(txn, operator_id, &inputs, by, map)?,
			Window {
				kind,
				group_by,
				aggregations,
				grace,
			} => self.add_window(operator_id, &inputs, kind, group_by, aggregations, grace, ctx)?,
		}

		Ok(())
	}

	#[inline]
	fn add_source_table(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		table: TableId,
	) -> Result<()> {
		let table = self.catalog.get_table(&mut txn.reborrow(), table)?;

		self.add_source(flow.id, operator_id, ObjectId::table(table.id));
		self.operators.insert(operator_id, Box::new(SourceTableOperator::new(operator_id, table)));
		Ok(())
	}

	#[inline]
	fn add_source_ringbuffer(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		ringbuffer: RingBufferId,
	) -> Result<()> {
		let rb = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?;
		self.add_source(flow.id, operator_id, ObjectId::ringbuffer(rb.id));
		self.operators.insert(operator_id, Box::new(SourceRingBufferOperator::new(operator_id, rb)));
		Ok(())
	}

	#[inline]
	fn add_source_series(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		series: SeriesId,
	) -> Result<()> {
		let s = self.catalog.get_series(&mut txn.reborrow(), series)?;
		self.add_source(flow.id, operator_id, ObjectId::series(s.id));
		self.operators.insert(operator_id, Box::new(SourceSeriesOperator::new(operator_id)));
		Ok(())
	}

	#[inline]
	fn add_filter(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(FilterOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_gate(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(GateOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_map(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(MapOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_extend(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(ExtendOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_sort(&mut self, operator_id: OperatorId, inputs: &[OperatorId]) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators
			.insert(operator_id, Box::new(SortOperator::new(parent_schema, operator_id, Vec::new())));
		Ok(())
	}

	#[inline]
	fn add_take(&mut self, operator_id: OperatorId, inputs: &[OperatorId], limit: usize) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(operator_id, Box::new(TakeOperator::new(parent_schema, operator_id, limit)));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_join(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		snapshot: bool,
		natural: bool,
		latest: bool,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		if inputs.len() != 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Join",
				expected: "exactly 2",
				found: inputs.len(),
			}));
		}

		let left_node = inputs[0];
		let right_node = inputs[1];

		let left_schema = self
			.operators
			.get(&left_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "left parent".to_string(),
				})
			})?
			.output_schema()
			.unwrap_or_default();

		let right_schema = self
			.operators
			.get(&right_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "right parent".to_string(),
				})
			})?
			.output_schema()
			.expect("right side of join must have a statically known schema");

		let (left_exprs, right_exprs) = if natural {
			let common = common_column_names(&left_schema, &right_schema);
			let keys: Vec<Expression> = common.iter().map(|name| natural_key_expr(name)).collect();
			(keys.clone(), keys)
		} else {
			(left, right)
		};

		let join_ttl = self.catalog.find_operator_settings(txn, operator_id)?.and_then(|s| s.join);
		let left = join_ttl.as_ref().and_then(|j| j.left.as_ref());
		let left_ttl = left.map(|t| t.duration);
		let right = join_ttl.as_ref().and_then(|j| j.right.as_ref());
		let right_ttl = right.map(|t| t.duration);

		self.operators.insert(
			operator_id,
			Box::new(JoinOperator::new(
				JoinSideConfig {
					schema: left_schema,
					operator: left_node,
					exprs: left_exprs,
				},
				JoinSideConfig {
					schema: right_schema,
					operator: right_node,
					exprs: right_exprs,
				},
				operator_id,
				join_type,
				alias,
				self.routines.clone(),
				self.runtime_context.clone(),
				snapshot,
				natural,
				latest,
				left_ttl,
				right_ttl,
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_distinct(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let ttl = self.operator_ttl(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			Box::new(DistinctOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
				ttl,
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_append(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
	) -> Result<()> {
		if inputs.len() < 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Append",
				expected: "at least 2",
				found: inputs.len(),
			}));
		}

		let mut parent_schemas = Vec::with_capacity(inputs.len());

		for input_node_id in inputs {
			let schema = self
				.operators
				.get(input_node_id)
				.ok_or_else(|| {
					Error::from(FlowGraphError::ParentOperatorNotFound {
						input: format!("{:?}", input_node_id),
					})
				})?
				.output_schema();
			parent_schemas.push(schema);
		}

		let parent_schema = parent_schemas.swap_remove(0);
		let ttl = self.operator_ttl(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			Box::new(AppendOperator::new(operator_id, parent_schema, inputs.to_vec(), ttl)),
		);
		Ok(())
	}

	#[inline]
	fn add_apply(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		operator: String,
		expressions: Vec<Expression>,
	) -> Result<()> {
		let config = evaluate_operator_config(expressions.as_slice(), &self.routines, &self.runtime_context)?;
		let cfg = Config::new(operator.as_str(), config);
		let ttl = self.operator_ttl(txn, operator_id)?;
		let parent_schema = self.parent_schema(first_input(inputs)?)?;

		let provider = self.operator_provider.clone();
		let inner = provider.provide(operator_id, &cfg)?;

		self.operators
			.insert(operator_id, Box::new(ApplyOperator::new(parent_schema, operator_id, inner, ttl)));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn add_window(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		kind: WindowKind,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		grace: Duration,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let operator = WindowOperator::new(WindowConfig {
			parent_schema,
			operator: operator_id,
			kind: kind.clone(),
			group_by: group_by.clone(),
			aggregations: aggregations.clone(),
			runtime_context: self.runtime_context.clone(),
			routines: self.routines.clone(),
			grace,
			ctx: Arc::clone(ctx),
		});
		self.operators.insert(operator_id, Box::new(operator));
		Ok(())
	}

	#[inline]
	fn add_aggregate(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		by: Vec<Expression>,
		map: Vec<Expression>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let operator = AggregateOperator::new(
			parent_schema,
			operator_id,
			by,
			map,
			self.routines.clone(),
			self.runtime_context.clone(),
			self.operator_ttl(txn, operator_id)?,
		);
		self.operators.insert(operator_id, Box::new(operator));
		Ok(())
	}

	fn operator_ttl(&self, txn: &mut Transaction<'_>, operator_id: OperatorId) -> Result<Option<Duration>> {
		Ok(self.catalog.find_operator_settings(txn, operator_id)?.and_then(|s| s.ttl).map(|ttl| ttl.duration))
	}

	fn require_parent(&self, input: OperatorId) -> Result<&BoxedOperator<T>> {
		self.operators.get(&input).ok_or_else(|| {
			Error::from(FlowGraphError::ParentOperatorNotFound {
				input: format!("{:?}", input),
			})
		})
	}

	fn parent_schema(&self, input: OperatorId) -> Result<Option<Columns>> {
		Ok(self.require_parent(input)?.output_schema())
	}

	#[inline]
	fn register_source_view(
		&mut self,
		txn: &mut Transaction<'_>,
		flow: &FlowDag,
		operator_id: OperatorId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, operator_id, ObjectId::view(view.id()));

		self.add_source(flow.id, operator_id, view.storage_id().into());

		self.operators.insert(operator_id, Box::new(SourceViewOperator::new(operator_id, view)));
		Ok(())
	}

	pub fn add_source(&mut self, flow: FlowId, operator: OperatorId, object: ObjectId) {
		let operators = self.sources.entry(object).or_default();

		let entry = (flow, operator);
		if !operators.contains(&entry) {
			operators.push(entry);
		}
	}

	pub fn add_sink(&mut self, flow: FlowId, operator: OperatorId, sink: ObjectId) {
		let operators = self.sinks.entry(sink).or_default();

		let entry = (flow, operator);
		if !operators.contains(&entry) {
			operators.push(entry);
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
