// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	flow::node::FlowNodeType::Take,
	nodes::{TakeLimit, TakeNode},
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct TakeCompiler {
	pub input: Box<QueryPlan>,
	pub limit: usize,
}

impl From<TakeNode> for TakeCompiler {
	fn from(node: TakeNode) -> Self {
		let limit = match node.take {
			TakeLimit::Literal(n) => n,
			TakeLimit::Variable(_) => unreachable!(),
		};
		Self {
			input: node.input,
			limit,
		}
	}
}

impl CompileOperator for TakeCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Take {
				limit: self.limit,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
