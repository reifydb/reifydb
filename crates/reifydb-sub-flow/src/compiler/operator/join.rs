// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	interface::{expression::Expression, FlowNodeId},
	JoinType,
};
use reifydb_rql::plan::physical::{JoinInnerNode, JoinLeftNode, PhysicalPlan};
use FlowNodeType::Operator;
use JoinType::{Inner, Left};
use OperatorType::Join;

use crate::{
	compiler::{CompileOperator, FlowCompiler}, FlowNodeType, OperatorType,
	Result,
};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub on: Vec<Expression>,
}

impl From<JoinInnerNode> for JoinCompiler {
	fn from(node: JoinInnerNode) -> Self {
		Self {
			join_type: Inner,
			left: node.left,
			right: node.right,
			on: node.on,
		}
	}
}

impl From<JoinLeftNode> for JoinCompiler {
	fn from(node: JoinLeftNode) -> Self {
		Self {
			join_type: Left,
			left: node.left,
			right: node.right,
			on: node.on,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(*self.left)?;
		let right_node = compiler.compile_plan(*self.right)?;

		compiler.build_node(Operator {
			operator: Join {
				join_type: self.join_type,
				left: self.on.clone(),
				right: self.on,
			},
		})
		.with_inputs([left_node, right_node])
		.build()
	}
}
