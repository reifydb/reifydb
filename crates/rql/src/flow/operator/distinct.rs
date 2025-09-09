// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{
		FlowNodeSchema, FlowNodeType::Operator, OperatorType::Distinct,
	},
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::{ColumnExpression, Expression},
	},
};
use reifydb_type::Fragment;

use super::super::{
	CompileOperator, FlowCompiler, conversion::to_owned_physical_plan,
};
use crate::{
	Result,
	plan::physical::{DistinctNode, PhysicalPlan},
};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan<'static>>,
	pub columns: Vec<Fragment<'static>>,
}

impl<'a> From<DistinctNode<'a>> for DistinctCompiler {
	fn from(node: DistinctNode<'a>) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node
				.columns
				.into_iter()
				.map(|f| Fragment::Owned(f.into_owned()))
				.collect(),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		// Convert column fragments to column expressions
		let expressions: Vec<Expression<'static>> = self
			.columns
			.into_iter()
			.map(|col| {
				Expression::Column(ColumnExpression(
					Fragment::Owned(col.into_owned()),
				))
			})
			.collect();

		compiler.build_node(Operator {
			operator: Distinct {
				expressions,
			},
			input_schemas: vec![FlowNodeSchema::empty()],
			output_schema: FlowNodeSchema::empty(),
		})
		.with_input(input_node)
		.build()
	}
}
