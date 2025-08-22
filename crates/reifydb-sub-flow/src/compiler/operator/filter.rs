// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	expression::Expression, CommandTransaction, FlowNodeId,
};
use reifydb_rql::plan::physical::{FilterNode, PhysicalPlan};
use FlowNodeType::Operator;
use OperatorType::Filter;

use crate::{
	compiler::{CompileOperator, FlowCompiler}, FlowNodeType, OperatorType,
	Result,
};

pub(crate) struct FilterCompiler {
	pub input: Box<PhysicalPlan>,
	pub conditions: Vec<Expression>,
}

impl From<FilterNode> for FilterCompiler {
	fn from(node: FilterNode) -> Self {
		Self {
			input: node.input,
			conditions: node.conditions,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for FilterCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		compiler.build_node(Operator {
			operator: Filter {
				conditions: self.conditions,
			},
		})
		.with_input(input_node)
		.build()
	}
}
