// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Apply;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};
use reifydb_type::Fragment;

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_fragment, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
	plan::physical::{ApplyNode, PhysicalPlan},
};

pub(crate) struct ApplyCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub operator: Fragment<'static>,
	pub arguments: Vec<Expression<'static>>,
}

impl<'a> From<ApplyNode<'a>> for ApplyCompiler {
	fn from(node: ApplyNode<'a>) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			operator: to_owned_fragment(node.operator),
			arguments: to_owned_expressions(node.expressions),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for ApplyCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(*input).await?)
		} else {
			None
		};

		let mut builder = compiler.build_node(Apply {
			operator: self.operator.text().to_string(),
			expressions: self.arguments,
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build().await
	}
}
