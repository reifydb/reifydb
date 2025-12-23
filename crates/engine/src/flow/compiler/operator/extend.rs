// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::Expression,
	flow::{
		FlowNodeType::Extend,
		conversion::{to_owned_expressions, to_owned_physical_plan},
	},
	plan::physical::{ExtendNode, PhysicalPlan},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input).await?)
		} else {
			None
		};

		let node_id = compiler
			.add_node(
				txn,
				Extend {
					expressions: self.expressions,
				},
			)
			.await?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id).await?;
		}

		Ok(node_id)
	}
}
