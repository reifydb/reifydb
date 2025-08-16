// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperatorType::{Filter, Map};
use reifydb_core::interface::{Evaluator, FlowId, FlowNodeId, SourceId};

use crate::{
	Flow, FlowNodeType, OperatorType,
	engine::FlowEngine,
	operator::{FilterOperator, MapOperator, Operator, OperatorContext},
};

impl<'a, E: Evaluator> FlowEngine<'a, E> {
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
					self.add_operator(node_id, operator)?;
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
		node: FlowNodeId,
		operator: &OperatorType,
	) -> crate::Result<()> {
		let operator = self.create_operator(operator.clone())?;
		debug_assert!(
			!self.operators.contains_key(&node),
			"Operator already registered"
		);

		self.operators.insert(node, operator);
		Ok(())
	}

	fn add_context(&'a mut self, node: FlowNodeId) {
		debug_assert!(
			!self.contexts.contains_key(&node),
			"Context already registered"
		);
		self.contexts
			.insert(node, OperatorContext::new(&self.evaluator));
	}

	fn create_operator(
		&self,
		operator: OperatorType,
	) -> crate::Result<Box<dyn Operator<E>>> {
		match operator {
			Filter {
				conditions,
			} => Ok(Box::new(FilterOperator::new(conditions))),
			Map {
				expressions,
			} => Ok(Box::new(MapOperator::new(expressions))),
			operator => unimplemented!("{:?}", operator),
		}
	}
}
