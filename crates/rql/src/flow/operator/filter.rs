// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Filter;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
	plan::physical::{FilterNode, PhysicalPlan},
};

pub(crate) struct FilterCompiler {
	pub input: Box<PhysicalPlan>,
	pub conditions: Vec<Expression>,
}

impl From<FilterNode> for FilterCompiler {
	fn from(node: FilterNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			conditions: to_owned_expressions(node.conditions),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for FilterCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input).await?;

		compiler.build_node(Filter {
			conditions: self.conditions,
		})
		.with_input(input_node)
		.build()
		.await
	}
}
