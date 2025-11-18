// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use FlowNodeType::{Aggregate, SinkView, SourceInlineData, SourceTable, SourceView};
use reifydb_catalog::{CatalogTableQueryOperations, CatalogViewQueryOperations, resolve::resolve_view};
use reifydb_core::{
	Error,
	interface::{FlowId, FlowNodeId, SourceId},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_rql::flow::{
	Flow, FlowNode, FlowNodeType,
	FlowNodeType::{Apply, Distinct, Extend, Filter, Join, Map, Sort, Take, Union, Window},
};
use reifydb_type::internal;

use super::eval::evaluate_operator_config;
use crate::{
	engine::FlowEngine,
	operator::{
		ApplyOperator, DistinctOperator, ExtendOperator, FilterOperator, JoinOperator, MapOperator, Operators,
		SinkViewOperator, SortOperator, SourceTableOperator, SourceViewOperator, TakeOperator, WindowOperator,
	},
};

impl FlowEngine {
	pub fn register(&self, txn: &mut StandardCommandTransaction, flow: Flow) -> crate::Result<()> {
		debug_assert!(!self.inner.flows.read().contains_key(&flow.id), "Flow already registered");

		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node)?;
		}

		// NEW: Load initial data from source tables
		self.load_initial_data(txn, &flow)?;

		// Add flow to analyzer for dependency tracking
		self.inner.analyzer.write().add(flow.clone());
		self.inner.flows.write().insert(flow.id, flow);

		Ok(())
	}

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
			// Union {} => Ok(Operators::Union(UnionOperator::new())),
			Union {} => unimplemented!(),
			Apply {
				operator_name,
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
				let operator = if self.is_ffi_operator(operator_name.as_str()) {
					let config = evaluate_operator_config(
						expressions.as_slice(),
						&self.inner.evaluator,
					)?;
					self.create_ffi_operator(operator_name.as_str(), node.id, &config)?
				} else {
					// Use registry for non-FFI operators
					self.inner.registry.create_operator(
						operator_name.as_str(),
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
