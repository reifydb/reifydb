// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Map;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
	plan::physical::{MapNode, PhysicalPlan},
};

pub(crate) struct MapCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub expressions: Vec<Expression<'static>>,
}

impl<'a> From<MapNode<'a>> for MapCompiler {
	fn from(node: MapNode<'a>) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			expressions: to_owned_expressions(node.map),
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

		let mut builder = compiler.build_node(Map {
			expressions: self.expressions,
		});

		if let Some(input) = input_node {
			builder = builder.with_input(input);
		}

		builder.build()
	}
}
