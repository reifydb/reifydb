// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeSchema, FlowNodeType::Operator, OperatorType::Take},
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{
	CompileOperator, FlowCompiler, conversion::to_owned_physical_plan,
};
use crate::{
	Result,
	plan::physical::{PhysicalPlan, TakeNode},
};

pub(crate) struct TakeCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub limit: usize,
}

impl<'a> From<TakeNode<'a>> for TakeCompiler {
	fn from(node: TakeNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			limit: node.take,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for TakeCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		compiler.build_node(Operator {
			operator: Take {
				limit: self.limit,
			},
			input_schemas: vec![FlowNodeSchema::empty()],
			output_schema: FlowNodeSchema::empty(),
		})
		.with_input(input_node)
		.build()
	}
}
