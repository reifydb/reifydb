// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of operator logical plans to FlowGraph nodes

use reifydb_core::SortKey;
use reifydb_rql::expression::Expression;

use super::FlowCompiler;
use crate::{NodeId, NodeType};

impl FlowCompiler {
	pub(crate) fn compile_filter(
		&mut self,
		conditions: Vec<Expression>,
		input_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the filter node
		let filter_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::Filter {
				predicate: conditions
					.into_iter()
					.next()
					.unwrap(), // Simplified - combine conditions
			},
		});

		// Connect input to filter
		self.flow.add_edge(&input_node, &filter_node)?;

		Ok(filter_node)
	}

	pub(crate) fn compile_map(
		&mut self,
		expressions: Vec<Expression>,
		input_node: Option<NodeId>,
	) -> crate::Result<NodeId> {
		// Create the map node
		let map_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::Map {
				expressions,
			},
		});

		// Connect input to map if present
		if let Some(input) = input_node {
			self.flow.add_edge(&input, &map_node)?;
		}

		Ok(map_node)
	}

	pub(crate) fn compile_aggregate(
		&mut self,
		by: Vec<Expression>,
		map: Vec<Expression>,
		input_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the aggregate node
		let agg_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::Aggregate {
				by,
				map,
			},
		});

		// Connect input to aggregate
		self.flow.add_edge(&input_node, &agg_node)?;

		Ok(agg_node)
	}

	pub(crate) fn compile_take(
		&mut self,
		limit: usize,
		input_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the take node using TopK operator
		let take_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::TopK {
				k: limit,
				sort: vec![], // No sorting, just limit
			},
		});

		// Connect input to take
		self.flow.add_edge(&input_node, &take_node)?;

		Ok(take_node)
	}

	pub(crate) fn compile_sort(
		&mut self,
		by: Vec<SortKey>,
		input_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the sort node using TopK operator with large k
		let sort_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::TopK {
				k: usize::MAX, // Sort all results
				sort: by,
			},
		});

		// Connect input to sort
		self.flow.add_edge(&input_node, &sort_node)?;

		Ok(sort_node)
	}

	pub(crate) fn compile_join_inner(
		&mut self,
		on: Vec<Expression>,
		left_node: NodeId,
		right_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the join node
		let join_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::Join {
				join_type: reifydb_core::JoinType::Inner,
				left: on.clone(),
				right: on,
			},
		});

		// Connect both inputs to join
		self.flow.add_edge(&left_node, &join_node)?;
		self.flow.add_edge(&right_node, &join_node)?;

		Ok(join_node)
	}

	pub(crate) fn compile_join_left(
		&mut self,
		on: Vec<Expression>,
		left_node: NodeId,
		right_node: NodeId,
	) -> crate::Result<NodeId> {
		// Create the join node
		let join_node = self.flow.add_node(NodeType::Operator {
			operator: crate::OperatorType::Join {
				join_type: reifydb_core::JoinType::Left,
				left: on.clone(),
				right: on,
			},
		});

		// Connect both inputs to join
		self.flow.add_edge(&left_node, &join_node)?;
		self.flow.add_edge(&right_node, &join_node)?;

		Ok(join_node)
	}
}
