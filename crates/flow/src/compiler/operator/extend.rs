// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{expression::Expression, flow::operator::OperatorDef::Extend, interface::catalog::flow::OperatorId};
use reifydb_rql::{
	expression::variant::{resolve_enum_equality, text_literal},
	nodes::ExtendNode,
	query::{QueryPlan, extract_resolved_source},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::compiler::{CompileOperator, FlowCompiler};

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
		let mut expressions = self.expressions;
		if let Some(source) = self.input.as_deref().and_then(extract_resolved_source) {
			let mut tx = txn.reborrow();
			for expr in &mut expressions {
				resolve_enum_equality(expr, &source, &compiler.catalog, &mut tx, &mut text_literal)?;
			}
		}
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Extend {
				expressions,
			},
		)?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id)?;
		}

		Ok(node_id)
	}
}
