// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeSchema, FlowNodeType::Operator, OperatorType::Aggregate},
	interface::{CommandTransaction, FlowNodeId, evaluate::expression::Expression},
};

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	plan::physical::{AggregateNode, PhysicalPlan},
};

pub(crate) struct AggregateCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub by: Vec<Expression<'static>>,
	pub map: Vec<Expression<'static>>,
}

impl<'a> From<AggregateNode<'a>> for AggregateCompiler {
	fn from(node: AggregateNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: to_owned_expressions(node.by),
			map: to_owned_expressions(node.map),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for AggregateCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		compiler.build_node(Operator {
			operator: Aggregate {
				by: self.by,
				map: self.map,
			},
			input_schemas: vec![FlowNodeSchema::empty()],
			output_schema: FlowNodeSchema::empty(),
		})
		.with_input(input_node)
		.build()
	}
}
