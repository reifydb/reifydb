// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	expression::Expression, CommandTransaction, FlowNodeId,
};
use reifydb_rql::plan::physical::{MapNode, PhysicalPlan};
use FlowNodeType::Operator;
use OperatorType::Map;

use crate::{
	compiler::{CompileOperator, FlowCompiler}, FlowNodeType, OperatorType,
	Result,
};

pub(crate) struct MapCompiler {
	pub input: Option<Box<PhysicalPlan>>,
	pub expressions: Vec<Expression>,
}

impl From<MapNode> for MapCompiler {
	fn from(node: MapNode) -> Self {
		Self {
			input: node.input,
			expressions: node.map,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for MapCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(*input)?)
		} else {
			None
		};

		let mut builder = compiler.build_node(Operator {
			operator: Map {
				expressions: self.expressions,
			},
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}
