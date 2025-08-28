// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowNodeType::Operator, OperatorType::Distinct},
	interface::{
		CommandTransaction, FlowNodeId,
		expression::{ColumnExpression, Expression},
	},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{
	Result,
	plan::physical::{DistinctNode, PhysicalPlan},
};

pub(crate) struct DistinctCompiler {
	pub input: Box<PhysicalPlan>,
	pub columns: Vec<reifydb_core::OwnedFragment>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: node.input,
			columns: node.columns,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(*self.input)?;

		// Convert column fragments to column expressions
		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(col)))
			.collect();

		compiler.build_node(Operator {
			operator: Distinct {
				expressions,
			},
		})
		.with_input(input_node)
		.build()
	}
}
