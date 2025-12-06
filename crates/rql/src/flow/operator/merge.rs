// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType, conversion::to_owned_physical_plan};
use crate::{
	Result,
	plan::physical::{MergeNode, PhysicalPlan},
};

pub(crate) struct MergeCompiler {
	pub left: Box<PhysicalPlan<'static>>,
	pub right: Box<PhysicalPlan<'static>>,
}

impl<'a> From<MergeNode<'a>> for MergeCompiler {
	fn from(node: MergeNode<'a>) -> Self {
		Self {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for MergeCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(*self.left)?;
		let right_node = compiler.compile_plan(*self.right)?;
		let node = compiler.build_node(FlowNodeType::Merge).with_inputs([left_node, right_node]).build()?;
		Ok(node)
	}
}
