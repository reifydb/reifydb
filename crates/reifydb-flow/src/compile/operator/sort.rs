// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Operator;
use OperatorType::Sort;
use reifydb_core::{
	SortKey,
	interface::{FlowNodeId, Transaction},
};
use reifydb_rql::plan::physical::{PhysicalPlan, SortNode};

use crate::{
	FlowNodeType, OperatorType, Result,
	compile::{CompileOperator, FlowCompiler},
};

pub(crate) struct SortCompiler {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<SortKey>,
}

impl From<SortNode> for SortCompiler {
	fn from(node: SortNode) -> Self {
		Self {
			input: node.input,
			by: node.by,
		}
	}
}

impl<T: Transaction> CompileOperator<T> for SortCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		compiler.build_node(Operator {
			operator: Sort {
				by: self.by,
			},
		})
		.with_input(input_node)
		.build()
	}
}
