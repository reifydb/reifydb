// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	expression::Expression,
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef::Extend,
	},
	nodes::ExtendNode,
	query::QueryPlan,
};

pub(crate) struct ExtendCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub expressions: Vec<Expression>,
}

impl From<ExtendNode> for ExtendCompiler {
	fn from(node: ExtendNode) -> Self {
		Self {
			input: node.input,
			expressions: node.extend,
		}
	}
}

impl CompileOperator for ExtendCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Extend {
				expressions: self.expressions,
			},
		)?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id)?;
		}

		Ok(node_id)
	}
}
