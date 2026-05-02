// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::store::ttl::create::create_operator_ttl;
use reifydb_core::{interface::catalog::flow::FlowNodeId, row::Ttl};
use reifydb_rql::{expression::Expression, flow::node::FlowNodeType::Apply, nodes::ApplyNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct ApplyCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub operator: Fragment,
	pub arguments: Vec<Expression>,
	pub ttl: Option<Ttl>,
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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let ttl = self.ttl.clone();
		let node_id = compiler.add_node(
			txn,
			Apply {
				operator: self.operator.text().to_string(),
				expressions: self.arguments,
				ttl: self.ttl,
			},
		)?;

		if let Some(ttl) = ttl
			&& let Transaction::Admin(admin) = txn
		{
			create_operator_ttl(admin, node_id, &ttl)?;
		}

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id)?;
		}

		Ok(node_id)
	}
}
