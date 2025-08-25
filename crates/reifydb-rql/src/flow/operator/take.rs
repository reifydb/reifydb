// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeType::Operator, OperatorType::Take},
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{
	Result,
	plan::physical::{PhysicalPlan, TakeNode},
};

pub(crate) struct TakeCompiler {
	pub input: Box<PhysicalPlan>,
	pub limit: usize,
}

impl From<TakeNode> for TakeCompiler {
	fn from(node: TakeNode) -> Self {
		Self {
			input: node.input,
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
		})
		.with_input(input_node)
		.build()
	}
}
