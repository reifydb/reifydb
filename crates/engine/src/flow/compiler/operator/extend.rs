// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	expression::Expression,
	flow::{
		conversion::{to_owned_expressions, to_owned_physical_plan},
		node::FlowNodeType::Extend,
	},
	nodes::{ExtendNode, PhysicalPlan},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct ExtendCompiler {
	pub input: Option<Box<PhysicalPlan>>,
	pub expressions: Vec<Expression>,
}

impl From<ExtendNode> for ExtendCompiler {
	fn from(node: ExtendNode) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			expressions: to_owned_expressions(node.extend),
		}
	}
}

impl CompileOperator for ExtendCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
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
