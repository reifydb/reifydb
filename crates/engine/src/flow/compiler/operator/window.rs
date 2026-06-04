// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::WindowKind, interface::catalog::flow::FlowNodeId};
use reifydb_rql::{expression::Expression, flow::node::FlowNodeType::Window, nodes::WindowNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{
	CompileOperator, FlowCompiler, operator::aggregate_validation::validate_flow_aggregations,
};

pub(crate) struct WindowCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
}

impl From<WindowNode> for WindowCompiler {
	fn from(node: WindowNode) -> Self {
		Self {
			input: node.input,
			kind: node.kind,
			group_by: node.group_by,
			aggregations: node.aggregations,
			ts: node.ts,
		}
	}
}

impl CompileOperator for WindowCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		validate_flow_aggregations(&self.aggregations)?;

		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Window {
				kind: self.kind,
				group_by: self.group_by,
				aggregations: self.aggregations,
				ts: self.ts,
			},
		)?;

		if let Some(input_node) = input_node {
			compiler.add_edge(txn, &input_node, &node_id)?;
		}

		Ok(node_id)
	}
}
