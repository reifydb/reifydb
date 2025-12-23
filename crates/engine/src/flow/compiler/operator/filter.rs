// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::Expression,
	flow::{
		FlowNodeType::Filter,
		conversion::{to_owned_expressions, to_owned_physical_plan},
	},
	plan::physical::{FilterNode, PhysicalPlan},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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

impl CompileOperator for FilterCompiler {
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input).await?;

		let node_id = compiler
			.add_node(
				txn,
				Filter {
					conditions: self.conditions,
				},
			)
			.await?;

		compiler.add_edge(txn, &input_node, &node_id).await?;
		Ok(node_id)
	}
}
