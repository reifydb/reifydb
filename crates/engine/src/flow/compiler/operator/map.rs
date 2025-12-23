// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::Expression,
	flow::{
		FlowNodeType::Map,
		conversion::{to_owned_expressions, to_owned_physical_plan},
	},
	plan::physical::{MapNode, PhysicalPlan},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

pub(crate) struct MapCompiler {
	pub input: Option<Box<PhysicalPlan>>,
	pub expressions: Vec<Expression>,
}

impl From<MapNode> for MapCompiler {
	fn from(node: MapNode) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			expressions: to_owned_expressions(node.map),
		}
	}
}

impl CompileOperator for MapCompiler {
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input).await?)
		} else {
			None
		};

		let node_id = compiler
			.add_node(
				txn,
				Map {
					expressions: self.expressions,
				},
			)
			.await?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id).await?;
		}

		Ok(node_id)
	}
}
