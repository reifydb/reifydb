// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::flow::FlowNodeId, sort::SortKey};
use reifydb_rql::{
	flow::{conversion::to_owned_physical_plan, node::FlowNodeType::Sort},
	plan::physical::{PhysicalPlan, SortNode},
};
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct SortCompiler {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<SortKey>,
}

impl From<SortNode> for SortCompiler {
	fn from(node: SortNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: node.by, // SortKey doesn't contain fragments
		}
	}
}

impl CompileOperator for SortCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut CommandTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Sort {
				by: self.by,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
