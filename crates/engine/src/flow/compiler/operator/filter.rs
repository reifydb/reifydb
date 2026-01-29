// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	expression::Expression,
	flow::{
		conversion::{to_owned_expressions, to_owned_physical_plan},
		node::FlowNodeType::Filter,
	},
	plan::physical::{FilterNode, PhysicalPlan},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Filter {
				conditions: self.conditions,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
