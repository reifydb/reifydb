// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Operator;
use OperatorType::Take;
use reifydb_core::interface::{FlowNodeId, Transaction};
use reifydb_rql::plan::physical::{PhysicalPlan, TakeNode};

use crate::{
	FlowNodeType, OperatorType, Result,
	compiler::{CompileOperator, FlowCompiler},
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

impl<T: Transaction> CompileOperator<T> for TakeCompiler {
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
