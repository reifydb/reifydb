// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use FlowNodeType::{Aggregate, SinkView, SourceFlow, SourceInlineData, SourceTable, SourceView};
use reifydb_catalog::{
	CatalogTableQueryOperations, CatalogViewQueryOperations, resolve::resolve_view,
	transaction::CatalogFlowQueryOperations,
};
use reifydb_core::{
	CommitVersion, Error,
	interface::{FlowId, FlowNodeId, SourceId},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_rql::flow::{
	Flow, FlowNode, FlowNodeType,
	FlowNodeType::{Apply, Distinct, Extend, Filter, Join, Map, Sort, Take, Union, Window},
};
use reifydb_type::internal;
use tracing::instrument;

use super::eval::evaluate_operator_config;
use crate::{
	engine::FlowEngine,
	operator::{
		ApplyOperator, DistinctOperator, ExtendOperator, FilterOperator, JoinOperator, MapOperator, Operators,
		SinkViewOperator, SortOperator, SourceFlowOperator, SourceTableOperator, SourceViewOperator,
		TakeOperator, UnionOperator, WindowOperator,
	},
};

impl FlowEngine {
	#[instrument(level = "info", skip(self, txn), fields(flow_id = ?flow.id))]
	pub fn register_without_backfill(&self, txn: &mut StandardCommandTransaction, flow: Flow) -> crate::Result<()> {
		self.register(txn, flow, None)
	}

	#[instrument(level = "info", skip(self, txn), fields(flow_id = ?flow.id, backfill_version = flow_creation_version.0))]
	pub fn register_with_backfill(
		&self,
		txn: &mut StandardCommandTransaction,
		flow: Flow,
		flow_creation_version: CommitVersion,
	) -> crate::Result<()> {
		self.register(txn, flow, Some(flow_creation_version))
	}

	#[instrument(level = "debug", skip(self, txn), fields(flow_id = ?flow.id, has_backfill = flow_creation_version.is_some()))]
	fn register(
		&self,
		txn: &mut StandardCommandTransaction,
		flow: Flow,
		flow_creation_version: Option<CommitVersion>,
	) -> crate::Result<()> {
		debug_assert!(!self.inner.flows.read().contains_key(&flow.id), "Flow already registered");

		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node)?;
		}

		if let Some(flow_creation_version) = flow_creation_version {
			self.inner.flow_creation_versions.write().insert(flow.id, flow_creation_version);

			if let Err(e) = self.load_initial_data(txn, &flow, flow_creation_version) {
				self.inner.flow_creation_versions.write().remove(&flow.id);
				return Err(e);
			}
		}

		// Add flow to analyzer for dependency tracking
		self.inner.analyzer.write().add(flow.clone());
		self.inner.flows.write().insert(flow.id, flow);

		Ok(())
	}

	#[instrument(level = "debug", skip(self, txn, flow), fields(flow_id = ?flow.id, node_id = ?node.id, node_type = ?std::mem::discriminant(&node.ty)))]
	fn add(&self, txn: &mut StandardCommandTransaction, flow: &Flow, node: &FlowNode) -> crate::Result<()> {
		debug_assert!(!self.inner.operators.read().contains_key(&node.id), "Operator already registered");
		let node = node.clone();

		match node.ty {
			SourceInlineData {
				..
			} => {
				unimplemented!()
			}
			SourceTable {
				table,
			} => {
				let table = txn.get_table(table)?;

				self.add_source(flow.id, node.id, SourceId::table(table.id));
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::SourceTable(SourceTableOperator::new(node.id, table))),
				);
			}
			SourceView {
				view,
			} => {
				let view = txn.get_view(view)?;
				self.add_source(flow.id, node.id, SourceId::view(view.id));
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::SourceView(SourceViewOperator::new(node.id, view))),
				);
			}
			SourceFlow {
				flow: source_flow,
			} => {
				let source_flow_def = txn.get_flow(source_flow)?;
				self.add_source(flow.id, node.id, SourceId::flow(source_flow_def.id));
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::SourceFlow(SourceFlowOperator::new(
						node.id,
						source_flow_def,
					))),
				);
			}
			SinkView {
				view,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				self.add_sink(flow.id, node.id, SourceId::view(*view));
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::SinkView(SinkViewOperator::new(
						parent,
						node.id,
						resolve_view(txn, view)?,
					))),
				);
			}
			Filter {
				conditions,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Filter(FilterOperator::new(parent, node.id, conditions))),
				);
			}
			Map {
				expressions,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Map(MapOperator::new(parent, node.id, expressions))),
				);
			}
			Extend {
				expressions,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Extend(ExtendOperator::new(parent, node.id, expressions))),
				);
			}
			Sort {
				by: _,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Sort(SortOperator::new(parent, node.id, Vec::new()))),
				);
			}
			Take {
				limit,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Take(TakeOperator::new(parent, node.id, limit))),
				);
			}
			Join {
				join_type,
				left,
				right,
				alias,
			} => {
				// Find the left and right node IDs from the flow inputs
				// The join node should have exactly 2 inputs
				if node.inputs.len() != 2 {
					return Err(Error(internal!("Join node must have exactly 2 inputs")));
				}

				let left_node = node.inputs[0];
				let right_node = node.inputs[1];

				let operators = self.inner.operators.read();
				let left_parent = operators
					.get(&left_node)
					.ok_or_else(|| Error(internal!("Left parent operator not found")))?
					.clone();

				let right_parent = operators
					.get(&right_node)
					.ok_or_else(|| Error(internal!("Right parent operator not found")))?
					.clone();
				drop(operators);

				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Join(JoinOperator::new(
						left_parent,
						right_parent,
						node.id,
						join_type,
						left_node,
						right_node,
						left,
						right,
						alias,
						self.inner.executor.clone(),
					))),
				);
			}
			Distinct {
				expressions,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Distinct(DistinctOperator::new(
						parent,
						node.id,
						expressions,
					))),
				);
			}
			Union {} => {
				// Union requires at least 2 inputs
				if node.inputs.len() < 2 {
					return Err(Error(internal!("Union node must have at least 2 inputs")));
				}

				let operators = self.inner.operators.read();
				let mut parents = Vec::with_capacity(node.inputs.len());

				for input_node_id in &node.inputs {
					let parent = operators
						.get(input_node_id)
						.ok_or_else(|| {
							Error(internal!(
								"Parent operator not found for input {:?}",
								input_node_id
							))
						})?
						.clone();
					parents.push(parent);
				}
				drop(operators);

				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Union(UnionOperator::new(
						node.id,
						parents,
						node.inputs.clone(),
					))),
				);
			}
			Apply {
				operator,
				expressions,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				// Check if this is an FFI operator and use the appropriate creation method
				let operator = if self.is_ffi_operator(operator.as_str()) {
					let config = evaluate_operator_config(
						expressions.as_slice(),
						&self.inner.evaluator,
					)?;
					self.create_ffi_operator(operator.as_str(), node.id, &config)?
				} else {
					// Use registry for non-FFI operators
					self.inner.registry.create_operator(
						operator.as_str(),
						node.id,
						expressions.as_slice(),
					)?
				};

				self.inner.operators.write().insert(
					node.id,
					Arc::new(Operators::Apply(ApplyOperator::new(parent, node.id, operator))),
				);
			}
			Aggregate {
				..
			} => unimplemented!(),
			Window {
				window_type,
				size,
				slide,
				group_by,
				aggregations,
				min_events,
				max_window_count,
				max_window_age,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				let operator = WindowOperator::new(
					parent,
					node.id,
					window_type.clone(),
					size.clone(),
					slide.clone(),
					group_by.clone(),
					aggregations.clone(),
					min_events.clone(),
					max_window_count.clone(),
					max_window_age.clone(),
				);
				self.inner.operators.write().insert(node.id, Arc::new(Operators::Window(operator)));
			}
		}

		Ok(())
	}

	fn add_source(&self, flow: FlowId, node: FlowNodeId, source: SourceId) {
		let mut sources = self.inner.sources.write();
		let nodes = sources.entry(source).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	fn add_sink(&self, flow: FlowId, node: FlowNodeId, sink: SourceId) {
		let mut sinks = self.inner.sinks.write();
		let nodes = sinks.entry(sink).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}
