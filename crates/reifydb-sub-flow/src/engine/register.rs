// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{
		Flow, FlowNodeType, OperatorType,
		OperatorType::{
			Aggregate, Distinct, Extend, Filter, Join, Map,
			MapTerminal, Sort, Take, Union,
		},
	},
	interface::{
		Evaluator, FlowId, FlowNodeId, QueryTransaction, SourceId,
	},
};

use crate::{
	engine::FlowEngine,
	operator::{
		AggregateOperator, DistinctOperator, ExtendOperator,
		FilterOperator, JoinOperator, MapOperator, MapTerminalOperator,
		OperatorEnum, SortOperator, TakeOperator, UnionOperator,
	},
};

impl<E: Evaluator> FlowEngine<E> {
	pub fn register(
		&mut self,
		txn: &mut impl QueryTransaction,
		flow: Flow<'static>,
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
						SourceId::Table(*table),
					);
				}
				FlowNodeType::Operator {
					operator,
				} => {
					self.add_operator(
						txn, flow.id, node_id, operator,
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

	fn add_operator<T: QueryTransaction>(
		&mut self,
		txn: &mut T,
		flow_id: FlowId,
		node: FlowNodeId,
		operator: &OperatorType<'static>,
	) -> crate::Result<()> {
		let operator = self.create_operator(
			txn,
			flow_id,
			node,
			operator.clone(),
		)?;
		debug_assert!(
			!self.operators.contains_key(&node),
			"Operator already registered"
		);

		self.operators.insert(node, operator);
		Ok(())
	}

	fn create_operator<T: QueryTransaction>(
		&self,
		txn: &mut T,
		flow_id: FlowId,
		node_id: FlowNodeId,
		operator: OperatorType<'static>,
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
			Extend {
				expressions,
			} => Ok(OperatorEnum::Extend(ExtendOperator::new(
				expressions,
			))),
			MapTerminal {
				expressions,
				view_id,
			} => {
				let view_def =
					CatalogStore::get_view(txn, view_id)?;
				Ok(OperatorEnum::MapTerminal(
					MapTerminalOperator::new(
						expressions,
						view_def,
					),
				))
			}
			Aggregate {
				by,
				map,
			} => Ok(OperatorEnum::Aggregate(
				AggregateOperator::new(
					flow_id.0, node_id.0, by, map,
				),
			)),
			Sort {
				by,
			} => Ok(OperatorEnum::Sort(SortOperator::new(by))),
			Take {
				limit,
			} => Ok(OperatorEnum::Take(TakeOperator::new(
				flow_id.0, node_id.0, limit,
			))),
			Join {
				join_type,
				left,
				right,
			} => Ok(OperatorEnum::Join(JoinOperator::new(
				join_type, left, right,
			))),
			Distinct {
				expressions,
			} => Ok(OperatorEnum::Distinct(DistinctOperator::new(
				flow_id.0,
				node_id.0,
				expressions,
			))),
			Union {} => {
				Ok(OperatorEnum::Union(UnionOperator::new()))
			}
		}
	}
}
