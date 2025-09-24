// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use JoinType::{Inner, Left};
use reifydb_core::{
	JoinType,
	interface::{CommandTransaction, FlowNodeId, evaluate::expression::Expression},
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
		// Compile with namespace tracking
		// let (left_node, left_schema) = compiler.compile_plan_with_definition(*self.left)?;
		// let (right_node, right_schema) = compiler.compile_plan_with_definition(*self.right)?;
		//
		// // Extract left and right keys from the join conditions
		// let (left_keys, right_keys) = extract_join_keys(&self.on);
		//
		// // Merge namespaces for output
		// let output_schema = FlowNodeDef::merge(&left_schema, &right_schema);
		//
		// compiler.build_node(Operator {
		// 	operator: Join {
		// 		join_type: self.join_type,
		// 		left: left_keys,
		// 		right: right_keys,
		// 	},
		// 	input_schemas: vec![left_schema, right_schema],
		// 	output_schema,
		// })
		// .with_inputs([left_node, right_node])
		// .build()
		unimplemented!()
	}
}

// Extract the left and right column references from join conditions
pub(crate) fn extract_join_keys(
	conditions: &[Expression<'static>],
) -> (Vec<Expression<'static>>, Vec<Expression<'static>>) {
	let mut left_keys = Vec::new();
	let mut right_keys = Vec::new();

	for condition in conditions {
		match condition {
			Expression::Equal(eq) => {
				// For equality conditions, extract the left and
				// right expressions
				left_keys.push(*eq.left.clone());
				right_keys.push(*eq.right.clone());
			}
			// For now, we only support simple equality joins
			// More complex conditions could be added later
			_ => {
				// If it's not an equality, we'll add the whole
				// condition to both sides This maintains
				// backwards compatibility but may not work
				// correctly
				left_keys.push(condition.clone());
				right_keys.push(condition.clone());
			}
		}
	}

	(left_keys, right_keys)
}
