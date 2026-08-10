// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, sort::SortKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef::Sort,
	},
	nodes::SortNode,
	query::QueryPlan,
};

pub(crate) struct SortCompiler {
	pub input: Box<QueryPlan>,
	pub by: Vec<SortKey>,
}

impl From<SortNode> for SortCompiler {
	fn from(node: SortNode) -> Self {
		Self {
			input: node.input,
			by: node.by,
		}
	}
}

impl CompileOperator for SortCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Sort {
				by: self.by,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
