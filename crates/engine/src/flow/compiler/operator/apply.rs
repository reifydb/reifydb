// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::Expression,
	flow::{
		FlowNodeType::Apply,
		conversion::{to_owned_expressions, to_owned_fragment, to_owned_physical_plan},
	},
	plan::physical::{ApplyNode, PhysicalPlan},
};
use reifydb_type::Fragment;

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

pub(crate) struct ApplyCompiler {
	pub input: Option<Box<PhysicalPlan>>,
	pub operator: Fragment,
	pub arguments: Vec<Expression>,
}

impl From<ApplyNode> for ApplyCompiler {
	fn from(node: ApplyNode) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			operator: to_owned_fragment(node.operator),
			arguments: to_owned_expressions(node.expressions),
		}
	}
}

impl CompileOperator for ApplyCompiler {
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
				Apply {
					operator: self.operator.text().to_string(),
					expressions: self.arguments,
				},
			)
			.await?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id).await?;
		}

		Ok(node_id)
	}
}
