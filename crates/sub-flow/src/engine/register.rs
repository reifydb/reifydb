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
use reifydb_type::internal_error;

use crate::{
	engine::FlowEngine,
	operator::{
		ApplyOperator, DistinctOperator, ExtendOperator, FilterOperator, JoinOperator, MapOperator, Operators,
		SinkViewOperator, SortOperator, SourceTableOperator, SourceViewOperator, TakeOperator, WindowOperator,
	},
};

impl FlowEngine {
	pub fn register(&mut self, txn: &mut StandardCommandTransaction, flow: Flow) -> crate::Result<()> {
		debug_assert!(!self.flows.contains_key(&flow.id), "Flow already registered");

		for node_id in flow.topological_order()? {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node)?;
		}

		// Add flow to analyzer for dependency tracking
		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow);

		Ok(())
	}

	fn add(&mut self, txn: &mut StandardCommandTransaction, flow: &Flow, node: &FlowNode) -> crate::Result<()> {
		debug_assert!(!self.operators.contains_key(&node.id), "Operator already registered");
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
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceTable(SourceTableOperator::new(node.id, table))),
				);
			}
			SourceView {
				view,
			} => {
				let view = txn.get_view(view)?;
				self.add_source(flow.id, node.id, SourceId::view(view.id));
				self.operators.insert(
					node.id,
					Arc::new(Operators::SourceView(SourceViewOperator::new(node.id, view))),
				);
			}
			SinkView {
				view,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();

				self.add_sink(flow.id, node.id, SourceId::view(*view));
				self.operators.insert(
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
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Filter(FilterOperator::new(parent, node.id, conditions))),
				);
			}
			Map {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Map(MapOperator::new(parent, node.id, expressions))),
				);
			}
			Extend {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Extend(ExtendOperator::new(parent, node.id, expressions))),
				);
			}
			Sort {
				by: _,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
					node.id,
					Arc::new(Operators::Sort(SortOperator::new(parent, node.id, Vec::new()))),
				);
			}
			Take {
				limit,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
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
					return Err(Error(internal_error!("Join node must have exactly 2 inputs")));
				}

				let left_node = node.inputs[0];
				let right_node = node.inputs[1];

				let left_parent = self
					.operators
					.get(&left_node)
					.ok_or_else(|| Error(internal_error!("Left parent operator not found")))?;

				let right_parent = self
					.operators
					.get(&right_node)
					.ok_or_else(|| Error(internal_error!("Right parent operator not found")))?;

				self.operators.insert(
					node.id,
					Arc::new(Operators::Join(JoinOperator::new(
						left_parent.clone(),
						right_parent.clone(),
						node.id,
						join_type,
						left_node,
						right_node,
						left,
						right,
						alias,
						self.executor.clone(),
					))),
				);
			}
			Distinct {
				expressions,
			} => {
				let parent = self
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				self.operators.insert(
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
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
					.clone();
				let operator = self.registry.create_operator(
					operator_name.as_str(),
					node.id,
					expressions.as_slice(),
				)?;

				self.operators.insert(
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
					.operators
					.get(&node.inputs[0])
					.ok_or_else(|| Error(internal_error!("Parent operator not found")))?
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
				self.operators.insert(node.id, Arc::new(Operators::Window(operator)));
			}
		}

		Ok(())
	}

	fn add_source(&mut self, flow: FlowId, node: FlowNodeId, source: SourceId) {
		let nodes = self.sources.entry(source).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	fn add_sink(&mut self, flow: FlowId, node: FlowNodeId, sink: SourceId) {
		let nodes = self.sinks.entry(sink).or_insert_with(Vec::new);

		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}
}
