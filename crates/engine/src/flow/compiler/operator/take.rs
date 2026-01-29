// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	flow::{conversion::to_owned_physical_plan, node::FlowNodeType::Take},
	plan::physical::{PhysicalPlan, TakeNode},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct TakeCompiler {
	pub input: Box<PhysicalPlan>,
	pub limit: usize,
}

impl From<TakeNode> for TakeCompiler {
	fn from(node: TakeNode) -> Self {
		Self {
			input: Box::new(to_owned_physical_plan(*node.input)),
			limit: node.take,
		}
	}
}

impl CompileOperator for TakeCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let node_id = compiler.add_node(
			txn,
			Take {
				limit: self.limit,
			},
		)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
