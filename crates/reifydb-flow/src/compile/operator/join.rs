// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Operator;
use JoinType::{Inner, Left};
use OperatorType::Join;
use reifydb_core::{
	JoinType,
	interface::{FlowNodeId, Transaction, expression::Expression},
};
use reifydb_rql::plan::physical::{JoinInnerNode, JoinLeftNode, PhysicalPlan};

use crate::{
	FlowNodeType, OperatorType, Result,
	compile::{CompileOperator, FlowCompiler},
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

impl<T: Transaction> CompileOperator<T> for JoinCompiler {
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
