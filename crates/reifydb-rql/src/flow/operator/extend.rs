// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeType::Operator, OperatorType::Extend},
	interface::{CommandTransaction, FlowNodeId, expression::Expression},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{
	Result,
	plan::physical::{ExtendNode, PhysicalPlan},
};

pub(crate) struct ExtendCompiler {
	pub input: Option<Box<PhysicalPlan>>,
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

impl<T: CommandTransaction> CompileOperator<T> for ExtendCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(*input)?)
		} else {
			None
		};

		let mut builder = compiler.build_node(Operator {
			operator: Extend {
				expressions: self.expressions,
			},
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}
