// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	expression::Expression, flow::node::FlowNodeType::Aggregate, nodes::AggregateNode, query::QueryPlan,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct AggregateCompiler {
	pub input: Box<QueryPlan>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

impl From<AggregateNode> for AggregateCompiler {
	fn from(node: AggregateNode) -> Self {
		Self {
			input: node.input,
			by: node.by,
			map: node.map,
		}
	}
}

impl CompileOperator for AggregateCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Aggregate {
				by: self.by,
				map: self.map,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
