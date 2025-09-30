// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Sort;
use reifydb_core::{
	SortKey,
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType, conversion::to_owned_physical_plan};
use crate::{
	Result,
	plan::physical::{PhysicalPlan, SortNode},
};

pub(crate) struct SortCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub by: Vec<SortKey>,
}

impl<'a> From<SortNode<'a>> for SortCompiler {
	fn from(node: SortNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: node.by, // SortKey doesn't contain fragments
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for SortCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		compiler.build_node(Sort {
			by: self.by,
		})
		.with_input(input_node)
		.build()
	}
}
