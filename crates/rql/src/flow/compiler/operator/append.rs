// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, row::OperatorTtl};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef,
	},
	nodes::AppendQueryNode,
	query::QueryPlan,
};

pub(crate) struct AppendCompiler {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub ttl: Option<OperatorTtl>,
}

impl From<AppendQueryNode> for AppendCompiler {
	fn from(node: AppendQueryNode) -> Self {
		Self {
			left: node.left,
			right: node.right,
			ttl: node.ttl,
		}
	}
}

impl CompileOperator for AppendCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let node_id = compiler.add_node(txn, OperatorDef::Append {})?;

		compiler.write_operator_settings(txn, node_id, self.ttl)?;

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
