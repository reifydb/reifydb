// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use JoinType::{Inner, Left};
use reifydb_core::{
	JoinType,
	flow::{FlowNodeType::Operator, OperatorType::Join},
	interface::{
		CommandTransaction, FlowNodeId,
		evaluate::expression::Expression,
	},
};

use super::super::{
	CompileOperator, FlowCompiler,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	plan::physical::{JoinInnerNode, JoinLeftNode, PhysicalPlan},
};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<PhysicalPlan<'static>>,
	pub right: Box<PhysicalPlan<'static>>,
	pub on: Vec<Expression<'static>>,
}

impl<'a> From<JoinInnerNode<'a>> for JoinCompiler {
	fn from(node: JoinInnerNode<'a>) -> Self {
		Self {
			join_type: Inner,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
		}
	}
}

impl<'a> From<JoinLeftNode<'a>> for JoinCompiler {
	fn from(node: JoinLeftNode<'a>) -> Self {
		Self {
			join_type: Left,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
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
