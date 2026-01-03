// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use FlowNodeType::{Aggregate, SinkSubscription, SinkView, SourceFlow, SourceInlineData, SourceTable, SourceView};
use reifydb_core::{
	Error,
	interface::{FlowId, FlowNodeId, PrimitiveId},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_rql::flow::{
	Flow, FlowNode, FlowNodeType,
	FlowNodeType::{Apply, Distinct, Extend, Filter, Join, Map, Merge, Sort, Take, Window},
};
use reifydb_type::internal;
use tracing::instrument;

use super::eval::evaluate_operator_config;
use crate::{
	engine::FlowEngine,
	operator::{
		ApplyOperator, DistinctOperator, ExtendOperator, FilterOperator, JoinOperator, MapOperator,
		MergeOperator, Operators, PrimitiveFlowOperator, PrimitiveTableOperator, PrimitiveViewOperator,
		SinkSubscriptionOperator, SinkViewOperator, SortOperator, TakeOperator, WindowOperator,
	},
};

impl FlowEngine {
	#[instrument(name = "flow::register", level = "debug", skip(self, txn), fields(flow_id = ?flow.id))]
	pub async fn register(&self, txn: &mut StandardCommandTransaction, flow: Flow) -> crate::Result<()> {
		debug_assert!(!self.inner.flows.read().await.contains_key(&flow.id), "Flow already registered");

		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node).await?;
		}

		self.inner.analyzer.write().await.add(flow.clone());
		self.inner.flows.write().await.insert(flow.id, flow);

		Ok(())
	}

	#[instrument(name = "flow::register::add_node", level = "debug", skip(self, txn, flow), fields(flow_id = ?flow.id, node_id = ?node.id, node_type = ?std::mem::discriminant(&node.ty)))]
	async fn add(&self, txn: &mut StandardCommandTransaction, flow: &Flow, node: &FlowNode) -> crate::Result<()> {
		debug_assert!(!self.inner.operators.read().await.contains_key(&node.id), "Operator already registered");
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
				let table = self.inner.catalog.get_table(txn, table).await?;

				self.add_source(flow.id, node.id, PrimitiveId::table(table.id)).await;
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::SourceTable(PrimitiveTableOperator::new(node.id, table))),
				);
			}
			SourceView {
				view,
			} => {
				let view = self.inner.catalog.get_view(txn, view).await?;
				self.add_source(flow.id, node.id, PrimitiveId::view(view.id)).await;
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::SourceView(PrimitiveViewOperator::new(node.id, view))),
				);
			}
			SourceFlow {
				flow: source_flow,
			} => {
				let source_flow_def = self.inner.catalog.get_flow(txn, source_flow).await?;
				self.add_source(flow.id, node.id, PrimitiveId::flow(source_flow_def.id)).await;
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::SourceFlow(PrimitiveFlowOperator::new(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				self.add_sink(flow.id, node.id, PrimitiveId::view(*view)).await;
				let resolved = self.inner.catalog.resolve_view(txn, view).await?;
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::SinkView(SinkViewOperator::new(parent, node.id, resolved))),
				);
			}
			SinkSubscription {
				subscription,
			} => {
				let parent = self
					.inner
					.operators
					.read()
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();

				// Note: Subscriptions use UUID-based IDs and are not added to the sinks map
				// which uses PrimitiveId (u64-based). Subscriptions are ephemeral 1:1 mapped.
				let resolved = self.inner.catalog.resolve_subscription(txn, subscription).await?;
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::SinkSubscription(SinkSubscriptionOperator::new(
						parent, node.id, resolved,
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
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

				let operators = self.inner.operators.read().await;
				let left_parent = operators
					.get(&left_node)
					.ok_or_else(|| Error(internal!("Left parent operator not found")))?
					.clone();

				let right_parent = operators
					.get(&right_node)
					.ok_or_else(|| Error(internal!("Right parent operator not found")))?
					.clone();
				drop(operators);

				self.inner.operators.write().await.insert(
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
					.await
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal!("Parent operator not found")))?
					.clone();
				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::Distinct(DistinctOperator::new(
						parent,
						node.id,
						expressions,
					))),
				);
			}
			Merge {} => {
				// Merge requires at least 2 inputs
				if node.inputs.len() < 2 {
					return Err(Error(internal!("Merge node must have at least 2 inputs")));
				}

				let operators = self.inner.operators.read().await;
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

				self.inner.operators.write().await.insert(
					node.id,
					Arc::new(Operators::Merge(MergeOperator::new(
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
					.await
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

				self.inner.operators.write().await.insert(
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
					.await
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
				self.inner
					.operators
					.write()
					.await
					.insert(node.id, Arc::new(Operators::Window(operator)));
			}
		}

		Ok(())
	}

	async fn add_source(&self, flow: FlowId, node: FlowNodeId, source: PrimitiveId) {
		let mut sources = self.inner.sources.write().await;
		let nodes = sources.entry(source).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	async fn add_sink(&self, flow: FlowId, node: FlowNodeId, sink: PrimitiveId) {
		let mut sinks = self.inner.sinks.write().await;
		let nodes = sinks.entry(sink).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}
