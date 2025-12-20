// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Aggregate;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
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
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input).await?;

		compiler.build_node(Aggregate {
			by: self.by,
			map: self.map,
		})
		.with_input(input_node)
		.build()
		.await
	}
}
