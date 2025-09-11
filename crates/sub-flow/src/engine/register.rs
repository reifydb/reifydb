// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{
		Flow, FlowNodeType, OperatorType,
		OperatorType::{
			Aggregate, Apply, Distinct, Extend, Filter, Join, Map,
			MapTerminal, Sort, Take, Union,
		},
	},
	interface::{CommandTransaction, FlowId, FlowNodeId, SourceId},
};

use crate::{
	engine::FlowEngine,
	operator::{
		AggregateOperator, DistinctOperator, ExtendOperator,
		FilterOperator, JoinOperator, MapOperator, MapTerminalOperator,
		Operators, SortOperator, TakeOperator, UnionOperator,
	},
};

impl<T: CommandTransaction> FlowEngine<T> {
	pub fn register(
		&mut self,
		txn: &mut T,
		flow: Flow,
	) -> crate::Result<()> {
		debug_assert!(
			!self.flows.contains_key(&flow.id),
			"Flow already registered"
		);

		for node_id in flow.get_node_ids() {
			let node = flow.get_node(&node_id).unwrap();
			match &node.ty {
				FlowNodeType::SourceInlineData {} => {
					unimplemented!()
				}
				FlowNodeType::SourceTable {
					table,
					..
				} => {
					self.add_source(
						flow.id,
						node_id,
						SourceId::from(*table),
					);
				}
				FlowNodeType::SourceView {
					view,
					..
				} => {
					self.add_source(
						flow.id,
						node_id,
						SourceId::from(*view),
					);
				}
				FlowNodeType::Operator {
					operator,
					input_schemas,
					output_schema,
				} => {
					self.add_operator(
						txn,
						flow.id,
						node_id,
						operator,
						input_schemas,
						output_schema,
					)?;
				}
				FlowNodeType::SinkView {
					view,
					..
				} => {
					self.add_sink(
						flow.id,
						node_id,
						SourceId::from(*view),
					);
				}
			}
		}

		self.flows.insert(flow.id, flow);

		Ok(())
	}

	fn add_source(
		&mut self,
		flow: FlowId,
		node: FlowNodeId,
		source: SourceId,
	) {
		use reifydb_core::log_debug;
		log_debug!(
			"FlowEngine: Registering flow {:?} node {:?} for source {:?}",
			flow,
			node,
			source
		);
		let nodes = self.sources.entry(source).or_insert_with(Vec::new);

		// Each node registration is unique
		// This allows multiple nodes in the same flow to listen to the
		// same source
		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
		log_debug!(
			"FlowEngine: Source {:?} now has {} dependent nodes",
			source,
			nodes.len()
		);
	}

	fn add_sink(&mut self, flow: FlowId, node: FlowNodeId, sink: SourceId) {
		let nodes = self.sinks.entry(sink).or_insert_with(Vec::new);

		// Each node registration is unique
		let entry = (flow, node);
		if !nodes.contains(&entry) {
			nodes.push(entry);
		}
	}

	fn add_operator(
		&mut self,
		txn: &mut T,
		flow_id: FlowId,
		node: FlowNodeId,
		operator: &OperatorType,
		input_schemas: &[reifydb_core::flow::FlowNodeSchema],
		output_schema: &reifydb_core::flow::FlowNodeSchema,
	) -> crate::Result<()> {
		let operator = self.create_operator(
			txn,
			flow_id,
			node,
			operator.clone(),
			input_schemas,
			output_schema,
		)?;
		debug_assert!(
			!self.operators.contains_key(&node),
			"Operator already registered"
		);

		self.operators.insert(node, operator);
		Ok(())
	}

	fn create_operator(
		&self,
		txn: &mut T,
		flow_id: FlowId,
		node_id: FlowNodeId,
		operator: OperatorType,
		input_schemas: &[reifydb_core::flow::FlowNodeSchema],
		_output_schema: &reifydb_core::flow::FlowNodeSchema,
	) -> crate::Result<Operators<T>> {
		match operator {
			Filter {
				conditions,
			} => Ok(Operators::Filter(FilterOperator::new(
				conditions,
			))),
			Map {
				expressions,
			} => Ok(Operators::Map(MapOperator::new(expressions))),
			Extend {
				expressions,
			} => Ok(Operators::Extend(ExtendOperator::new(
				expressions,
			))),
			MapTerminal {
				expressions,
				view_id,
			} => {
				let view_def =
					CatalogStore::get_view(txn, view_id)?;
				Ok(Operators::MapTerminal(
					MapTerminalOperator::new(
						expressions,
						view_def,
					),
				))
			}
			Aggregate {
				by,
				map,
			} => Ok(Operators::Aggregate(AggregateOperator::new(
				flow_id.0, node_id.0, by, map,
			))),
			Sort {
				by,
			} => Ok(Operators::Sort(SortOperator::new(by))),
			Take {
				limit,
			} => Ok(Operators::Take(TakeOperator::new(
				flow_id.0, node_id.0, limit,
			))),
			Join {
				join_type,
				left,
				right,
			} => {
				// Extract schemas for left and right inputs
				let left_schema = if input_schemas.len() > 0 {
					input_schemas[0].clone()
				} else {
					reifydb_core::flow::FlowNodeSchema::empty()
				};
				let right_schema = if input_schemas.len() > 1 {
					input_schemas[1].clone()
				} else {
					reifydb_core::flow::FlowNodeSchema::empty()
				};

				Ok(Operators::Join(
					JoinOperator::new(
						join_type,
						left,
						right,
						left_schema,
						right_schema,
					)
					.with_flow_id(flow_id.0)
					.with_instance_id(node_id.0),
				))
			}
			Distinct {
				expressions,
			} => Ok(Operators::Distinct(DistinctOperator::new(
				flow_id.0,
				node_id.0,
				expressions,
			))),
			Union {} => Ok(Operators::Union(UnionOperator::new())),
			Apply {
				operator_name,
				expressions,
			} => {
				// Apply uses the ApplyOperator from the apply
				// module
				use crate::operator::ApplyOperator;

				let operator = self.registry.create_operator(
					operator_name.as_str(),
					node_id,
					expressions.as_slice(),
				)?;

				Ok(Operators::Apply(ApplyOperator::new(
					operator,
				)))
			}
		}
	}
}
