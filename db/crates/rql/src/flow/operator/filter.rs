// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeSchema, FlowNodeType::Operator, OperatorType::Filter},
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::Expression,
	},
};

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	plan::physical::{FilterNode, PhysicalPlan},
};

pub(crate) struct FilterCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub conditions: Vec<Expression<'static>>,
}

impl<'a> From<FilterNode<'a>> for FilterCompiler {
	fn from(node: FilterNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			conditions: to_owned_expressions(node.conditions),
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
			input_schemas: vec![FlowNodeSchema::empty()],
			output_schema: FlowNodeSchema::empty(),
		})
		.with_input(input_node)
		.build()
	}
}
