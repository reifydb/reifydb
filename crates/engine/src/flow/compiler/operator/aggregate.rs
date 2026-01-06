// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	expression::Expression,
	flow::{
		FlowNodeType::Aggregate,
		conversion::{to_owned_expressions, to_owned_physical_plan},
	},
	plan::physical::{AggregateNode, PhysicalPlan},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

pub(crate) struct AggregateCompiler {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

impl From<AggregateNode> for AggregateCompiler {
	fn from(node: AggregateNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: to_owned_expressions(node.by),
			map: to_owned_expressions(node.map),
		}
	}
}

impl CompileOperator for AggregateCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Aggregate {
				by: self.by,
				map: self.map,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
