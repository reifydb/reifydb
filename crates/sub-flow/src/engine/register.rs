// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::{Aggregate, SinkView, SourceInlineData, SourceTable, SourceView};
use reifydb_catalog::resolve::resolve_view;
use reifydb_core::{
	Error,
	interface::{FlowId, FlowNodeId, SourceId, Transaction},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_rql::{
	flow::{
		Flow, FlowNode, FlowNodeType,
		FlowNodeType::{Apply, Distinct, Extend, Filter, Join, Map, Sort, Take, Union},
	},
	plan::physical::{InlineDataNode, PhysicalPlan},
};
use reifydb_type::internal_error;

use crate::{
	engine::FlowEngine,
	operator::{
		ApplyOperator, DistinctOperator, ExtendOperator, FilterOperator, JoinOperator, MapOperator, Operators,
		SinkViewOperator, SortOperator, TakeOperator,
	},
};

impl<T: Transaction> FlowEngine<T> {
	pub fn register(&mut self, txn: &mut StandardCommandTransaction<T>, flow: Flow) -> crate::Result<()> {
		debug_assert!(!self.flows.contains_key(&flow.id), "Flow already registered");

		for node_id in flow.get_node_ids() {
			let node = flow.get_node(&node_id).unwrap();
			self.add(txn, &flow, node)?;
		}

		self.flows.insert(flow.id, flow);

		Ok(())
	}

	fn add(&mut self, txn: &mut StandardCommandTransaction<T>, flow: &Flow, node: &FlowNode) -> crate::Result<()> {
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
				self.add_source(flow.id, node.id, SourceId::table(*table));
			}
			SourceView {
				view,
			} => {
				self.add_source(flow.id, node.id, SourceId::view(*view));
			}
			SinkView {
				view,
			} => {
				self.add_sink(flow.id, node.id, SourceId::view(*view));
				self.operators.insert(
					node.id,
					Operators::SinkView(SinkViewOperator::new(node.id, resolve_view(txn, view)?)),
				);
			}
			Filter {
				conditions,
			} => {
				self.operators
					.insert(node.id, Operators::Filter(FilterOperator::new(node.id, conditions)));
			}
			Map {
				expressions,
			} => {
				self.operators.insert(node.id, Operators::Map(MapOperator::new(node.id, expressions)));
			}
			Extend {
				expressions,
			} => {
				self.operators
					.insert(node.id, Operators::Extend(ExtendOperator::new(node.id, expressions)));
			}
			Sort {
				by: _,
			} => {
				self.operators.insert(node.id, Operators::Sort(SortOperator::new(node.id, Vec::new())));
			}
			Take {
				limit,
			} => {
				self.operators.insert(node.id, Operators::Take(TakeOperator::new(node.id, limit)));
			}
			Join {
				join_type,
				left,
				right,
				alias,
				strategy,
				right_plan,
			} => {
				// Find the left and right node IDs from the flow inputs
				// The join node should have exactly 2 inputs
				if node.inputs.len() != 2 {
					return Err(Error(internal_error!("Join node must have exactly 2 inputs")));
				}

				let left_node = node.inputs[0];
				let right_node = node.inputs[1];

				// Extract the right_plan if it exists, otherwise create a default empty plan
				let plan = right_plan.unwrap_or_else(|| {
					PhysicalPlan::InlineData(InlineDataNode {
						rows: vec![],
					})
				});

				self.operators.insert(
					node.id,
					Operators::Join(JoinOperator::new(
						node.id, join_type, left_node, right_node, left, right, plan, alias,
						strategy,
					)),
				);
			}
			Distinct {
				expressions,
			} => {
				self.operators.insert(
					node.id,
					Operators::Distinct(DistinctOperator::new(node.id, expressions)),
				);
			}
			// Union {} => Ok(Operators::Union(UnionOperator::new())),
			Union {} => unimplemented!(),
			Apply {
				operator_name,
				expressions,
			} => {
				let operator = self.registry.create_operator(
					operator_name.as_str(),
					node.id,
					expressions.as_slice(),
				)?;

				self.operators.insert(node.id, Operators::Apply(ApplyOperator::new(node.id, operator)));
			}
			Aggregate {
				..
			} => unimplemented!(),
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
