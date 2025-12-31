// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{Result, SortKey, interface::FlowNodeId};
use reifydb_rql::{
	flow::{FlowNodeType::Sort, conversion::to_owned_physical_plan},
	plan::physical::{PhysicalPlan, SortNode},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input).await?;

		let node_id = compiler
			.add_node(
				txn,
				Sort {
					by: self.by,
				},
			)
			.await?;

		compiler.add_edge(txn, &input_node, &node_id).await?;
		Ok(node_id)
	}
}
