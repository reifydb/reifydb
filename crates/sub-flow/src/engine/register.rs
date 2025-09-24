// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::{Aggregate, SinkView, SourceInlineData, SourceTable, SourceView};
use reifydb_catalog::resolve::resolve_view;
use reifydb_core::{
	flow::{
		Flow, FlowNode, FlowNodeType,
		FlowNodeType::{Apply, Distinct, Extend, Filter, Join, Map, Sort, Take, Union},
	},
	interface::{FlowId, FlowNodeId, SourceId, Transaction},
};
use reifydb_engine::StandardCommandTransaction;

use crate::{
	engine::FlowEngine,
	operator::{
		DistinctOperator, ExtendOperator, FilterOperator, MapOperator, Operators, SinkViewOperator,
		SortOperator, TakeOperator,
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
					Operators::SinkView(SinkViewOperator::new(resolve_view(txn, view)?)),
				);
			}
			Filter {
				conditions,
			} => {
				self.operators.insert(node.id, Operators::Filter(FilterOperator::new(conditions)));
			}
			Map {
				expressions,
			} => {
				self.operators.insert(node.id, Operators::Map(MapOperator::new(expressions)));
			}
			Extend {
				expressions,
			} => {
				self.operators.insert(node.id, Operators::Extend(ExtendOperator::new(expressions)));
			}
			Sort {
				by: _,
			} => {
				self.operators.insert(node.id, Operators::Sort(SortOperator::new(Vec::new())));
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
			} => {
				// Ok(Operators::Join(
				// 	JoinOperator::new(node_id, join_type, left, right, left_schema, right_schema)
				// 		.with_flow_id(flow_id.0)
				// 		.with_instance_id(node_id.0),
				// ))
				unimplemented!()
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
				let _operator = self.registry.create_operator(
					operator_name.as_str(),
					node.id,
					expressions.as_slice(),
				)?;

				// Ok(Operators::Apply(ApplyOperator::new(operator)))
				unimplemented!()
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
