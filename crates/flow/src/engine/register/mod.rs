// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod config;
mod sink;
mod source;
mod transform;

use std::{mem, sync::Arc};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	value::column::columns::Columns,
};
use reifydb_rql::flow::{
	flow::FlowDag,
	operator::{
		FlowNode,
		OperatorDef::{
			Aggregate, Append, Apply, Distinct, Extend, Filter, Gate, Join, Map, SinkRingBufferView,
			SinkSeriesView, SinkSubscription, SinkTableView, Sort, SourceInlineData, SourceRingBuffer,
			SourceSeries, SourceTable, SourceView, Take, Window,
		},
	},
	time_domain::{check_join_seal_requirements, check_window_time_requirements},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{Result, error::Error, reifydb_assertions, value::duration::Duration};
use tracing::{info, instrument};

use crate::{
	context::FlowContext,
	engine::FlowEngineInner,
	error::FlowGraphError,
	operator::BoxedHostOperator,
	timer::TimerDue,
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

		check_window_time_requirements(&self.catalog, txn, &flow)?;
		check_join_seal_requirements(&self.catalog, txn, &flow)?;

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

		let wheel = self.substrate.timers.clone();
		let store = self.substrate.operators.as_ref().expect("flow engine was built without an operator store");
		let armed: Vec<TimerDue> = flow
			.get_operator_ids()
			.filter_map(|operator_id| wheel.next_due_stored(operator_id, store))
			.collect();
		self.timers.rebuild(flow.id, armed);

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
			} => self.add_source_view(txn, flow, operator_id, view)?,
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
				seal,
			} => self.add_window(operator_id, &inputs, kind, group_by, aggregations, seal, ctx)?,
		}

		Ok(())
	}

	fn operator_seal(&self, txn: &mut Transaction<'_>, operator_id: OperatorId) -> Result<Option<Duration>> {
		Ok(self.catalog
			.find_operator_settings(txn, operator_id)?
			.and_then(|s| s.seal)
			.map(|seal| seal.duration))
	}

	fn require_parent(&self, input: OperatorId) -> Result<&BoxedHostOperator> {
		self.operators.get(&input).ok_or_else(|| {
			Error::from(FlowGraphError::ParentOperatorNotFound {
				input: format!("{:?}", input),
			})
		})
	}

	fn parent_schema(&self, input: OperatorId) -> Result<Option<Columns>> {
		Ok(self.require_parent(input)?.output_schema())
	}
}

fn first_input(inputs: &[OperatorId]) -> Result<OperatorId> {
	inputs.first().copied().ok_or_else(|| Error::from(FlowGraphError::MissingInputEdge))
}
