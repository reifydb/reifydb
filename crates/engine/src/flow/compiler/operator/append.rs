// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType, nodes::AppendQueryNode, query::QueryPlan};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct AppendCompiler {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
}

impl From<AppendQueryNode> for AppendCompiler {
	fn from(node: AppendQueryNode) -> Self {
		Self {
			left: node.left,
			right: node.right,
		}
	}
}

impl CompileOperator for AppendCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let node_id = compiler.add_node(txn, FlowNodeType::Append)?;

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
