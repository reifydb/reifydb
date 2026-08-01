// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_catalog::vtable::system::node_retention_store::NodeRetentionInfo;
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
use reifydb_engine::flow::time_domain::check_time_domain;
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
use reifydb_sdk::config::Config;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{Result, error::Error, fragment::Fragment, reifydb_assertions, value::duration::Duration};
use tracing::instrument;

use super::eval::evaluate_operator_config;
use crate::{
	context::FlowContext,
	engine::{FlowEngineInner, state_lease_default},
	error::FlowGraphError,
	operator::{
		OperatorCell,
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

		check_time_domain(&self.catalog, txn, &flow)?;

		let mut added: Vec<OperatorId> = Vec::new();
		let ctx = Arc::new(FlowContext::default());
		for operator_id in flow.topological_order()? {
			let operator = flow.get_operator(&operator_id).unwrap();
			if let Err(err) = self.add(txn, &flow, operator, &ctx) {
				for id in &added {
					self.operators.remove(id);
					self.executor.services().node_retention_store.remove(*id);
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
			self.check_declared_span(&flow, operator)?;
			self.adopt_horizon(operator);
			added.push(operator_id);
		}

		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow.clone());
		self.execution_level_cache.invalidate();
		self.schedule_cache.invalidate();

		Ok(())
	}

	fn check_declared_span(&self, flow: &FlowDag, operator: &FlowNode) -> Result<()> {
		let Some(settings) = self.catalog.find_operator_settings_latest(operator.id) else {
			return Ok(());
		};
		if settings.ttl.is_none() && settings.join.is_none() {
			return Ok(());
		}
		if !operator.ty.consults_declared_span() {
			return Err(FlowGraphError::SpanOnUnageableNode {
				flow_id: flow.id.0,
				operator: operator.ty.label(),
			}
			.into());
		}
		let reclaims = self
			.operators
			.get(&operator.id)
			.is_some_and(|operator| operator.capabilities().contains(&OperatorCapability::Reclaim));
		if !reclaims {
			return Err(FlowGraphError::SpanWithoutReclaim {
				flow_id: flow.id.0,
				operator: operator.ty.label(),
			}
			.into());
		}
		Ok(())
	}

	fn adopt_horizon(&self, operator: &FlowNode) {
		let scale = self.node_retention_scale(operator);
		self.substrate.group.set_activity_grid(operator.id, scale);
		self.executor.services().node_retention_store.set(NodeRetentionInfo {
			operator: operator.id,
			stateful: operator.ty.holds_state(),
			scale,
			frontier: None,
		});
	}

	pub(crate) fn node_retention_scale(&self, operator: &FlowNode) -> Option<Duration> {
		self.operators.get(&operator.id).and_then(|operator| operator.retention_scale())
	}

	#[instrument(name = "flow::add", level = "debug", skip(self, txn, flow, ctx), fields(flow_id = ?flow.id, operator_id = ?operator.id, node_type = ?mem::discriminant(&operator.ty)))]
	pub fn add(
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
			} => self.add_source_table(txn, flow, operator_id, table)?,
			SourceView {
				view,
			} => self.register_source_view(txn, flow, operator_id, view)?,
			SourceRingBuffer {
				ringbuffer,
			} => self.add_source_ringbuffer(txn, flow, operator_id, ringbuffer)?,
			SourceSeries {
				series,
			} => self.add_source_series(txn, flow, operator_id, series)?,
			SinkTableView {
				view,
				table,
			} => self.add_sink_table_view(txn, flow, operator_id, &inputs, view, table)?,
			SinkRingBufferView {
				view,
				ringbuffer,
				capacity,
			} => self.add_sink_ringbuffer_view(txn, flow, operator_id, &inputs, view, ringbuffer, capacity)?,
			SinkSeriesView {
				view,
				series,
				key,
			} => self.add_sink_series_view(txn, flow, operator_id, &inputs, view, series, key)?,
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
		self.operators.insert(operator_id, OperatorCell::new(SourceTableOperator::new(operator_id, table)));
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
		self.operators.insert(operator_id, OperatorCell::new(SourceRingBufferOperator::new(operator_id, rb)));
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
		self.operators.insert(operator_id, OperatorCell::new(SourceSeriesOperator::new(operator_id)));
		Ok(())
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
		let parent = self.parent(first_input(inputs)?)?;

		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_table(&mut txn.reborrow(), table)?.partition_by;
		self.operators.insert(
			operator_id,
			OperatorCell::new(SinkTableViewOperator::new(
				parent,
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
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_ringbuffer(&mut txn.reborrow(), ringbuffer)?.partition_by;
		let ttl = self
			.catalog
			.find_row_settings(&mut txn.reborrow(), StorageId::ringbuffer(ringbuffer))?
			.and_then(|settings| settings.ttl);
		let announce_evictions = ttl.as_ref().map(|ttl| ttl.announce).unwrap_or(true);
		let row_ttl = ttl.as_ref().map(|t| t.duration);
		self.operators.insert(
			operator_id,
			OperatorCell::new(SinkRingBufferViewOperator::new(
				parent,
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
		let parent = self.parent(first_input(inputs)?)?;
		self.add_sink(flow.id, operator_id, ObjectId::view(*view));
		let resolved = self.catalog.resolve_view(&mut txn.reborrow(), view)?;
		let partition_by = self.catalog.get_series(&mut txn.reborrow(), series)?.partition_by;
		self.operators.insert(
			operator_id,
			OperatorCell::new(SinkSeriesViewOperator::new(
				parent,
				operator_id,
				resolved,
				series,
				key.clone(),
				partition_by,
			)),
		);
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
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(FilterOperator::new(
				parent,
				operator_id,
				conditions,
				self.executor.routines.clone(),
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
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(GateOperator::new(
				parent,
				operator_id,
				conditions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
				self.state_budget.clone(),
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
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(MapOperator::new(
				parent,
				operator_id,
				expressions,
				self.executor.routines.clone(),
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
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(ExtendOperator::new(
				parent,
				operator_id,
				expressions,
				self.executor.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	fn add_sort(&mut self, operator_id: OperatorId, inputs: &[OperatorId]) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators
			.insert(operator_id, OperatorCell::new(SortOperator::new(parent, operator_id, Vec::new())));
		Ok(())
	}

	#[inline]
	fn add_take(&mut self, operator_id: OperatorId, inputs: &[OperatorId], limit: usize) -> Result<()> {
		let parent = self.parent(first_input(inputs)?)?;
		self.operators.insert(operator_id, OperatorCell::new(TakeOperator::new(parent, operator_id, limit)));
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

		let join_ttl = self.catalog.find_operator_settings(txn, operator_id)?.and_then(|s| s.join);
		let left = join_ttl.as_ref().and_then(|j| j.left.as_ref());
		let left_ttl = left.map(|t| t.duration);
		let right = join_ttl.as_ref().and_then(|j| j.right.as_ref());
		let right_ttl = right.map(|t| t.duration);

		self.operators.insert(
			operator_id,
			OperatorCell::new(JoinOperator::new(
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
				self.executor.clone(),
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
		let parent = self.parent(first_input(inputs)?)?;
		let ttl = self.operator_ttl(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(DistinctOperator::new(
				parent,
				operator_id,
				expressions,
				self.executor.routines.clone(),
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

		let ttl = self.operator_ttl(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			OperatorCell::new(AppendOperator::new(operator_id, parents, inputs.to_vec(), ttl)),
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
		let config = evaluate_operator_config(
			expressions.as_slice(),
			&self.executor.routines,
			&self.runtime_context,
		)?;
		let cfg = Config::new(operator.as_str(), config.clone());
		let ttl = self.operator_ttl(txn, operator_id)?;

		if let Some(factory) = self.custom_operators.get(operator.as_str()) {
			let parent = self.parent(first_input(inputs)?)?;
			let _lease = self.state_budget.grant_lease(operator_id, state_lease_default());
			let inner = match factory(operator_id, &cfg) {
				Ok(op) => op,
				Err(e) => {
					self.state_budget.release_lease(operator_id);
					return Err(e);
				}
			};
			self.operators.insert(
				operator_id,
				OperatorCell::new(ApplyOperator::new(parent, operator_id, inner, ttl)),
			);
		} else {
			#[cfg(reifydb_target = "native")]
			{
				let parent = self.parent(first_input(inputs)?)?;

				let inner = if self.is_native_operator(operator.as_str()) {
					self.create_native_operator(operator.as_str(), operator_id, &cfg)?
				} else if self.is_ffi_operator(operator.as_str()) {
					self.create_ffi_operator(operator.as_str(), operator_id, &config)?
				} else {
					return Err(Error::from(FlowGraphError::UnknownOperator {
						operator: operator.to_string(),
					}));
				};

				self.operators.insert(
					operator_id,
					OperatorCell::new(ApplyOperator::new(parent, operator_id, inner, ttl)),
				);
			}
			#[cfg(not(reifydb_target = "native"))]
			{
				let _ = (operator, inputs);

				return Err(Error::from(FlowGraphError::FfiUnsupportedOnWasm));
			}
		}
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
		let parent = self.parent(first_input(inputs)?)?;
		let operator = WindowOperator::new(WindowConfig {
			parent,
			operator: operator_id,
			kind: kind.clone(),
			group_by: group_by.clone(),
			aggregations: aggregations.clone(),
			runtime_context: self.runtime_context.clone(),
			routines: self.executor.routines.clone(),
			grace,
			state_budget: self.state_budget.clone(),
			ctx: Arc::clone(ctx),
		});
		self.operators.insert(operator_id, OperatorCell::new(operator));
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
		let parent = self.parent(first_input(inputs)?)?;
		let operator = AggregateOperator::new(
			parent,
			operator_id,
			by,
			map,
			self.executor.routines.clone(),
			self.runtime_context.clone(),
			self.operator_ttl(txn, operator_id)?,
		);
		self.operators.insert(operator_id, OperatorCell::new(operator));
		Ok(())
	}

	fn operator_ttl(&self, txn: &mut Transaction<'_>, operator_id: OperatorId) -> Result<Option<Duration>> {
		Ok(self.catalog.find_operator_settings(txn, operator_id)?.and_then(|s| s.ttl).map(|ttl| ttl.duration))
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
		operator_id: OperatorId,
		view: ViewId,
	) -> Result<()> {
		let view = self.catalog.get_view(&mut txn.reborrow(), view)?;
		self.add_source(flow.id, operator_id, ObjectId::view(view.id()));

		self.add_source(flow.id, operator_id, view.storage_id().into());

		self.operators.insert(operator_id, OperatorCell::new(SourceViewOperator::new(operator_id, view)));
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
