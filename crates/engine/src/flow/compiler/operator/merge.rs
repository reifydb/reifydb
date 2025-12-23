// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{
	flow::{FlowNodeType, conversion::to_owned_physical_plan},
	plan::physical::{MergeNode, PhysicalPlan},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

pub(crate) struct MergeCompiler {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
}

impl From<MergeNode> for MergeCompiler {
	fn from(node: MergeNode) -> Self {
		Self {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
		}
	}
}

impl CompileOperator for MergeCompiler {
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(txn, *self.left).await?;
		let right_node = compiler.compile_plan(txn, *self.right).await?;

		let node_id = compiler.add_node(txn, FlowNodeType::Merge).await?;

		compiler.add_edge(txn, &left_node, &node_id).await?;
		compiler.add_edge(txn, &right_node, &node_id).await?;

		Ok(node_id)
	}
}
