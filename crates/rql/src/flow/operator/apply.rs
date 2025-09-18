// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType::Operator, OperatorType::Apply},
	interface::{CommandTransaction, FlowNodeId, evaluate::expression::Expression},
};
use reifydb_type::Fragment;

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_fragment, to_owned_physical_plan},
};
use crate::{
	Result,
	plan::physical::{ApplyNode, PhysicalPlan},
};

pub(crate) struct ApplyCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub operator_name: Fragment<'static>,
	pub arguments: Vec<Expression<'static>>,
}

impl<'a> From<ApplyNode<'a>> for ApplyCompiler {
	fn from(node: ApplyNode<'a>) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			operator_name: to_owned_fragment(node.operator),
			arguments: to_owned_expressions(node.expressions),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for ApplyCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(*input)?)
		} else {
			None
		};

		let mut builder = compiler.build_node(Operator {
			operator: Apply {
				operator_name: self.operator_name.fragment().to_string(),
				expressions: self.arguments,
			},
			input_schemas: vec![],
			output_schema: FlowNodeDef::empty(),
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}
