// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let node_id = compiler.add_node(txn, FlowNodeType::Merge)?;

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
