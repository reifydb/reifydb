// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, row::OperatorTtl};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, fragment::Fragment};

use crate::{
	expression::Expression,
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef::Apply,
	},
	nodes::ApplyNode,
	query::QueryPlan,
};

pub(crate) struct ApplyCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub operator: Fragment,
	pub arguments: Vec<Expression>,
	pub ttl: Option<OperatorTtl>,
}

impl From<ApplyNode> for ApplyCompiler {
	fn from(node: ApplyNode) -> Self {
		Self {
			input: node.input,
			operator: node.operator,
			arguments: node.expressions,
			ttl: node.ttl,
		}
	}
}

impl CompileOperator for ApplyCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Apply {
				operator: self.operator.text().to_string(),
				expressions: self.arguments,
			},
		)?;

		compiler.write_operator_settings(txn, node_id, self.ttl)?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id)?;
		}

		Ok(node_id)
	}
}
