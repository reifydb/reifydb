// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeType::Operator, OperatorType::Aggregate},
	interface::{CommandTransaction, FlowNodeId, expression::Expression},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{
	Result,
	plan::physical::{AggregateNode, PhysicalPlan},
};

pub(crate) struct AggregateCompiler {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

impl From<AggregateNode> for AggregateCompiler {
	fn from(node: AggregateNode) -> Self {
		Self {
			input: node.input,
			by: node.by,
			map: node.map,
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
		})
		.with_input(input_node)
		.build()
	}
}
