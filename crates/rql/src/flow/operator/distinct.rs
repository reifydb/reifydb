// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::FlowNodeType::Distinct,
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::{ColumnExpression, Expression},
		identifier::ColumnIdentifier,
	},
};

use super::super::{CompileOperator, FlowCompiler, conversion::to_owned_physical_plan};
use crate::{
	Result,
	plan::physical::{DistinctNode, PhysicalPlan},
};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub columns: Vec<ColumnIdentifier<'static>>,
}

impl<'a> From<DistinctNode<'a>> for DistinctCompiler {
	fn from(node: DistinctNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node.columns.into_iter().map(|c| c.into_owned()).collect(),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		// Convert column identifiers to column expressions
		let expressions: Vec<Expression<'static>> =
			self.columns.into_iter().map(|col| Expression::Column(ColumnExpression(col))).collect();

		compiler.build_node(Distinct {
			expressions,
		})
		.with_input(input_node)
		.build()
	}
}
