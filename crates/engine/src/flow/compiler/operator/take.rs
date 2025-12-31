// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	flow::{FlowNodeType::Take, conversion::to_owned_physical_plan},
	plan::physical::{PhysicalPlan, TakeNode},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let input_node = compiler.compile_plan(txn, *self.input).await?;

		let node_id = compiler
			.add_node(
				txn,
				Take {
					limit: self.limit,
				},
			)
			.await?;

		compiler.add_edge(txn, &input_node, &node_id).await?;
		Ok(node_id)
	}
}
