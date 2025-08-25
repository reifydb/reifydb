// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{
		Flow, FlowNodeType, OperatorType,
		OperatorType::{Aggregate, Filter, Map},
	},
	interface::{Evaluator, FlowId, FlowNodeId, SourceId},
};

use crate::{
	engine::FlowEngine,
	operator::{
		AggregateOperator, FilterOperator, MapOperator, OperatorEnum,
	},
};

impl<E: Evaluator> FlowEngine<E> {
	pub fn register(&mut self, flow: Flow) -> crate::Result<()> {
		debug_assert!(
			!self.flows.contains_key(&flow.id),
			"Flow already registered"
		);

		for node_id in flow.get_node_ids() {
			let node = flow.get_node(&node_id).unwrap();
			match &node.ty {
				FlowNodeType::SourceTable {
					table,
					..
				} => {
					self.add_source(
						flow.id,
						SourceId::Table(*table),
					);
				}
				FlowNodeType::Operator {
					operator,
				} => {
					self.add_operator(
						flow.id, node_id, operator,
					)?;
				}
				FlowNodeType::SinkView {
					view,
					..
				} => {
					self.add_sink(
						flow.id,
						SourceId::View(*view),
					);
				}
			}
		}

		self.flows.insert(flow.id, flow);

		Ok(())
	}

	fn add_source(&mut self, flow: FlowId, source: SourceId) {
		let flows = self.sources.entry(source).or_insert_with(Vec::new);

		debug_assert!(
			!flows.contains(&flow),
			"Flow {:?} already registered for source {:?}",
			flow,
			source
		);

		flows.push(flow);
	}

	fn add_sink(&mut self, flow: FlowId, sink: SourceId) {
		let flows = self.sinks.entry(sink).or_insert_with(Vec::new);

		debug_assert!(
			!flows.contains(&flow),
			"Flow {:?} already registered for sink {:?}",
			flow,
			sink
		);

		flows.push(flow);
	}

	fn add_operator(
		&mut self,
		flow_id: FlowId,
		node: FlowNodeId,
		operator: &OperatorType,
	) -> crate::Result<()> {
		let operator =
			self.create_operator(flow_id, node, operator.clone())?;
		debug_assert!(
			!self.operators.contains_key(&node),
			"Operator already registered"
		);

		self.operators.insert(node, operator);
		Ok(())
	}

	fn create_operator(
		&self,
		flow_id: FlowId,
		node_id: FlowNodeId,
		operator: OperatorType,
	) -> crate::Result<OperatorEnum<E>> {
		match operator {
			Filter {
				conditions,
			} => Ok(OperatorEnum::Filter(FilterOperator::new(
				conditions,
			))),
			Map {
				expressions,
			} => Ok(OperatorEnum::Map(MapOperator::new(
				expressions,
			))),
			Aggregate {
				by,
				map,
			} => Ok(OperatorEnum::Aggregate(
				AggregateOperator::new(
					flow_id.0, node_id.0, by, map,
				),
			)),
			operator => unimplemented!("{:?}", operator),
		}
	}
}
