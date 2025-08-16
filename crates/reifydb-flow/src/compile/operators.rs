// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of operator logical plans to FlowGraph nodes

use reifydb_catalog::sequence::flow::{next_flow_edge_id, next_flow_node_id};
use reifydb_core::{
	SortKey,
	interface::{
		ActiveCommandTransaction, FlowNodeId, Transaction,
		expression::Expression,
	},
};

use super::FlowCompiler;
use crate::{FlowEdge, FlowNode, FlowNodeType};

impl FlowCompiler {
	pub(crate) fn compile_filter<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		conditions: Vec<Expression>,
		input_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the filter node
		let filter_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::Filter {
					predicate: conditions
						.into_iter()
						.next()
						.unwrap(), /* Simplified - combine
					             * conditions */
				},
			},
		));

		// Connect input to filter
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&input_node,
			&filter_node,
		))?;

		Ok(filter_node)
	}

	pub(crate) fn compile_map<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		expressions: Vec<Expression>,
		input_node: Option<FlowNodeId>,
	) -> crate::Result<FlowNodeId> {
		// Create the map node
		let map_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::Map {
					expressions,
				},
			},
		));

		// Connect input to map if present
		if let Some(input) = input_node {
			self.flow.add_edge(FlowEdge::new(
				next_flow_edge_id(txn)?,
				&input,
				&map_node,
			))?;
		}

		Ok(map_node)
	}

	pub(crate) fn compile_aggregate<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		by: Vec<Expression>,
		map: Vec<Expression>,
		input_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the aggregate node
		let agg_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::Aggregate {
					by,
					map,
				},
			},
		));

		// Connect input to aggregate
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&input_node,
			&agg_node,
		))?;

		Ok(agg_node)
	}

	pub(crate) fn compile_take<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		limit: usize,
		input_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the take node using TopK operator
		let take_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::TopK {
					k: limit,
					sort: vec![], // No sorting, just limit
				},
			},
		));

		// Connect input to take
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&input_node,
			&take_node,
		))?;

		Ok(take_node)
	}

	pub(crate) fn compile_sort<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		by: Vec<SortKey>,
		input_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the sort node using TopK operator with large k
		let sort_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::TopK {
					k: usize::MAX, // Sort all results
					sort: by,
				},
			},
		));

		// Connect input to sort
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&input_node,
			&sort_node,
		))?;

		Ok(sort_node)
	}

	pub(crate) fn compile_join_inner<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		on: Vec<Expression>,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the join node
		let join_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::Join {
					join_type:
						reifydb_core::JoinType::Inner,
					left: on.clone(),
					right: on,
				},
			},
		));

		// Connect both inputs to join
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&left_node,
			&join_node,
		))?;
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&right_node,
			&join_node,
		))?;

		Ok(join_node)
	}

	pub(crate) fn compile_join_left<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		on: Vec<Expression>,
		left_node: FlowNodeId,
		right_node: FlowNodeId,
	) -> crate::Result<FlowNodeId> {
		// Create the join node
		let join_node = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::Operator {
				operator: crate::OperatorType::Join {
					join_type: reifydb_core::JoinType::Left,
					left: on.clone(),
					right: on,
				},
			},
		));

		// Connect both inputs to join
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&left_node,
			&join_node,
		))?;
		self.flow.add_edge(FlowEdge::new(
			next_flow_edge_id(txn)?,
			&right_node,
			&join_node,
		))?;

		Ok(join_node)
	}
}
